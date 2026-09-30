SELECT id, txn_type, amount, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS rn FROM transactions WHERE account_id IN (8, 16, 18, 34);
