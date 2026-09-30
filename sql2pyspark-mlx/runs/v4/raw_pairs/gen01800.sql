SELECT merchant, CAST(is_flagged AS INT) AS is_flagged_flag, CONCAT_WS('-', txn_type, merchant) AS txn_type_merchant_key FROM transactions WHERE HOUR(txn_time) BETWEEN 10 AND 18
