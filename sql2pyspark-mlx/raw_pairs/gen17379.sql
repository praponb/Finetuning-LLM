SELECT
    a.id,
    a.balance,
    t.stddev_amount,
    t.amount_range
FROM accounts a
INNER JOIN (SELECT account_id, ROUND(STDDEV(amount), 2) AS stddev_amount, MAX(amount) - MIN(amount) AS amount_range
    FROM transactions
    WHERE TO_DATE(txn_time) >= '2025-02-27'
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.amount_range >= 75
ORDER BY t.amount_range ASC, a.balance ASC
