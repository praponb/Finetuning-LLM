SELECT id, account_id, UPPER(txn_type) AS txn_type_upper FROM transactions WHERE account_id IN (2, 37) AND amount BETWEEN 3400 AND 3800 AND is_flagged = TRUE ORDER BY account_id DESC;
