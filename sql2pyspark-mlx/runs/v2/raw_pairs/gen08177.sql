SELECT *, CAST(is_flagged AS INT) is_flagged_flag, CONCAT_WS('-', txn_type, merchant) txn_type_merchant_key FROM transactions WHERE is_flagged = TRUE;
