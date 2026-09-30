SELECT account_id, amount, merchant, LOWER(merchant) merchant_lower FROM transactions WHERE amount <= 3100
