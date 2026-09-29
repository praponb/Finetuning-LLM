WITH customer_summary AS (
    SELECT customer_id, COUNT(*) AS cnt
    FROM subscriptions
    WHERE monthly_fee < 80
    GROUP BY customer_id
)
SELECT
    c.id,
    c.name,
    c.credit_limit,
    cs.cnt
FROM customers c
JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.cnt >= 7
ORDER BY c.id;
