SELECT id, account_id, txn_type, CAST(is_flagged AS INT) AS is_flagged_flag, SUBSTRING(merchant, 1, 4) AS merchant_prefix FROM transactions
