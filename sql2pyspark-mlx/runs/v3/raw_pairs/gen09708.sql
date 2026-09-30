WITH customer_summary AS (
    SELECT customer_id, MAX(started_at) AS last_started_at
    FROM sessions
    WHERE browser IN ('Firefox', 'Safari', 'Chrome')
    GROUP BY customer_id
)
SELECT
    c.id,
    c.segment,
    c.name,
    cs.last_started_at
FROM customers c
INNER JOIN customer_summary cs ON c.id = cs.customer_id
