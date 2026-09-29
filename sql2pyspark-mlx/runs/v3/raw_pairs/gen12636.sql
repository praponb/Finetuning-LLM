WITH customer_stats AS (
    SELECT customer_id, ROUND(AVG(balance), 2) AS avg_balance, MAX(balance) - MIN(balance) AS balance_range
    FROM accounts
    WHERE status IS NOT NULL
    GROUP BY customer_id
)
SELECT
    c.is_active,
    c.name,
    cs.avg_balance,
    cs.balance_range
FROM customers c
LEFT JOIN customer_stats cs ON c.id = cs.customer_id
