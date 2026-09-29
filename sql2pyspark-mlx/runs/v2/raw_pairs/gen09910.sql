SELECT id, amount, txn_type, FIRST_VALUE(txn_type) OVER (ORDER BY amount, id) AS first_txn_type FROM transactions WHERE account_id IN (7, 15, 28, 40);
