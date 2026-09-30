SELECT id, account_id, merchant, TO_DATE(txn_time) txn_time_date FROM transactions WHERE amount BETWEEN 1900 AND 2000 ORDER BY id DESC, account_id LIMIT 10;
