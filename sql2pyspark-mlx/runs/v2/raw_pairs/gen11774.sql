WITH customer_summary AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(device))) AS device_values, SORT_ARRAY(COLLECT_SET(country)) AS country_list
    FROM sessions
    WHERE device = 'mobile'
    GROUP BY customer_id
)
SELECT c.segment, cs.device_values, cs.country_list
FROM customers c
INNER JOIN customer_summary cs ON c.id = cs.customer_id
ORDER BY c.segment DESC, device_values
