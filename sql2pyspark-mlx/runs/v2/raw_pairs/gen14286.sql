SELECT txn_type, COUNT(DISTINCT amount) AS unique_accounts FROM transactions WHERE (account_id IN (2, 11, 18, 37) AND merchant = 'Amazon') OR merchant = 'Amazon' GROUP BY txn_type;
