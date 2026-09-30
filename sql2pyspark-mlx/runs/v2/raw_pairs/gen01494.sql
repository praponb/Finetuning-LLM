SELECT id, amount, txn_time
FROM transactions
WHERE amount <= (SELECT MIN(amount) FROM transactions WHERE txn_time >= '2024-12-04')
ORDER BY amount, id DESC
