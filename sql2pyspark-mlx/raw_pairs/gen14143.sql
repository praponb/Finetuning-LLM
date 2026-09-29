WITH customer_summary AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(device))) AS device_values
    FROM sessions
    GROUP BY customer_id
)
SELECT c.id, c.name, cs.device_values
FROM customers c
JOIN customer_summary cs ON c.id = cs.customer_id
