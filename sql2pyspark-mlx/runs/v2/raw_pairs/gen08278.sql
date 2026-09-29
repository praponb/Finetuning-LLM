SELECT
    c.id,
    c.credit_limit,
    c.name,
    t.stddev_balance,
    t.total_balance
FROM customers c
LEFT JOIN (SELECT customer_id, ROUND(STDDEV(balance), 2) AS stddev_balance, SUM(balance) AS total_balance
    FROM accounts
    GROUP BY customer_id) t ON c.id = t.customer_id
ORDER BY c.id DESC, stddev_balance DESC;
