SELECT id, merchant, txn_type, account_id, TRIM(merchant) AS clean_merchant FROM transactions ORDER BY txn_type;
