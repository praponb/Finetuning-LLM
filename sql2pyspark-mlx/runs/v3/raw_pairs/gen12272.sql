WITH customer_summary AS (
    SELECT customer_id, SORT_ARRAY(COLLECT_SET(device)) device_list
    FROM sessions
    WHERE started_at >= '2025-10-28'
    GROUP BY customer_id
)
SELECT
    c.id,
    c.signup_date,
    c.segment,
    cs.device_list
FROM customers c
LEFT JOIN customer_summary cs ON c.id = cs.customer_id
