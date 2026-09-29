SELECT id, amount, txn_time
FROM transactions
WHERE amount > (SELECT MAX(amount) FROM transactions WHERE txn_time < '2024-11-01')
