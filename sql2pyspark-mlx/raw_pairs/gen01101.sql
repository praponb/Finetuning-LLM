SELECT id, txn_time, account_id, LOWER(merchant) AS merchant_lower FROM transactions ORDER BY merchant_lower
