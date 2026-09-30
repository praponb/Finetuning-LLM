SELECT
    a.balance,
    a.status,
    t.stddev_amount,
    t.amount_range
FROM accounts a
INNER JOIN (SELECT account_id, ROUND(STDDEV(amount), 2) AS stddev_amount, MAX(amount) - MIN(amount) AS amount_range
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.amount_range > 57
ORDER BY balance
