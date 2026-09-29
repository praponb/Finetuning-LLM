SELECT COUNT(*) AS total_count, MIN(amount) AS min_amount
FROM transactions
WHERE amount <= 2700 AND (amount <= 3500 OR txn_time >= '2024-09-06')
