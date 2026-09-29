SELECT
    id,
    amount,
    is_flagged,
    txn_time
FROM transactions
WHERE amount > (SELECT AVG(amount) FROM transactions)
ORDER BY txn_time ASC, id ASC
LIMIT 5
