SELECT id, is_flagged, account_id, UPPER(txn_type) AS txn_type_upper FROM transactions ORDER BY id LIMIT 20
