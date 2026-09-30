SELECT id, txn_type, merchant, LOWER(merchant) AS merchant_lower FROM transactions WHERE merchant = 'Lazada';
