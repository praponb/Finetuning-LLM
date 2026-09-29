SELECT
    a.id,
    a.customer_id,
    t.avg_amount,
    t.stddev_amount
FROM accounts a
LEFT JOIN (SELECT account_id, AVG(amount) AS avg_amount, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM transactions
    WHERE amount > 1500
    GROUP BY account_id) t ON a.id = t.account_id
