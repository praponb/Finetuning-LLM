SELECT id, txn_type, CAST(is_flagged AS INT) is_flagged_flag FROM transactions ORDER BY is_flagged_flag DESC;
