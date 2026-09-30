WITH customer_stats AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(device))) AS device_values, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values
    FROM sessions
    GROUP BY customer_id
)
SELECT c.email, cs.device_values, cs.country_values
FROM customers c
LEFT JOIN customer_stats cs ON c.id = cs.customer_id
