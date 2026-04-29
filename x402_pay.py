"""
x402_pay.py — Python EIP-3009 payment helper for Hive agents
=============================================================
Produces a base64-encoded X-PAYMENT header that HiveCompute's x402
middleware accepts. Signs a transferWithAuthorization (EIP-3009) using
eth_account — no Node.js required.

Usage:
    from x402_pay import build_payment_header, PRICE_USDC
    headers = { "X-PAYMENT": build_payment_header(price_usdc=0.01), ... }
"""

import os, time, secrets, json, base64
from eth_account import Account
from eth_account.messages import encode_typed_data

# ── Config ─────────────────────────────────────────────────────────────────────
AGENT_PK      = os.environ.get(
    "AGENT_WALLET_PK",
    "0xa50726073d9bb635fd05e1aa73bdd1e4bc7c45761a6fec2d0b182c87d46299db"
)
TREASURY      = os.environ.get("HOUSE_WALLET", "0x15184bf50b3d3f52b60434f8942b7d52f2eb436e")
USDC_CONTRACT = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
CHAIN_ID      = 8453          # Base mainnet
PRICE_USDC    = 0.01          # $0.01 per inference call

# USDC on Base EIP-712 domain
EIP712_DOMAIN = {
    "name":              "USD Coin",
    "version":           "2",
    "chainId":           CHAIN_ID,
    "verifyingContract": USDC_CONTRACT,
}

TRANSFER_WITH_AUTH_TYPES = {
    "EIP712Domain": [
        {"name": "name",              "type": "string"},
        {"name": "version",           "type": "string"},
        {"name": "chainId",           "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ],
    "TransferWithAuthorization": [
        {"name": "from",        "type": "address"},
        {"name": "to",          "type": "address"},
        {"name": "value",       "type": "uint256"},
        {"name": "validAfter",  "type": "uint256"},
        {"name": "validBefore", "type": "uint256"},
        {"name": "nonce",       "type": "bytes32"},
    ],
}


def build_payment_header(price_usdc: float = PRICE_USDC) -> str:
    """
    Build a base64-encoded X-PAYMENT header containing a signed EIP-3009
    TransferWithAuthorization. Compatible with HiveCompute x402 middleware.
    """
    acct       = Account.from_key(AGENT_PK)
    value      = int(price_usdc * 1_000_000)          # atomic USDC (6 decimals)
    now        = int(time.time())
    valid_after  = now - 30                            # 30s grace for clock skew
    valid_before = now + 300                           # 5 min window
    nonce      = "0x" + secrets.token_hex(32)          # random bytes32

    message = {
        "from":        acct.address,
        "to":          TREASURY,
        "value":       value,
        "validAfter":  valid_after,
        "validBefore": valid_before,
        "nonce":       nonce,
    }

    structured = {
        "domain":      EIP712_DOMAIN,
        "types":       TRANSFER_WITH_AUTH_TYPES,
        "primaryType": "TransferWithAuthorization",
        "message":     message,
    }

    signable  = encode_typed_data(full_message=structured)
    signed    = acct.sign_message(signable)
    signature = signed.signature.hex()
    if not signature.startswith("0x"):
        signature = "0x" + signature

    payload = {
        "x402Version": 1,
        "scheme":      "exact",
        "network":     "base",
        "payload": {
            "authorization": {
                "from":        acct.address,
                "to":          TREASURY,
                "value":       value,
                "validAfter":  valid_after,
                "validBefore": valid_before,
                "nonce":       nonce,
            },
            "signature": signature,
        },
        "payer": acct.address,
    }

    return base64.b64encode(json.dumps(payload).encode()).decode()


def payment_headers(price_usdc: float = PRICE_USDC) -> dict:
    """Return a dict with the X-PAYMENT header ready to merge into request headers."""
    return {"X-PAYMENT": build_payment_header(price_usdc)}
