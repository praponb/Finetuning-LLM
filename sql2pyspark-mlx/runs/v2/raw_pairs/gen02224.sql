WITH customer_stats AS (
    SELECT customer_id, SUM(CASE WHEN device = 'tablet' THEN 1 ELSE 0 END) AS tablet_count
    FROM sessions
    GROUP BY customer_id
)
SELECT
    c.id,
    c.credit_limit,
    c.is_active,
    cs.tablet_count
FROM customers c
LEFT JOIN customer_stats cs ON c.id = cs.customer_id
ORDER BY cs.tablet_count DESC, c.id;
