SELECT
    customers.id,
    customers.country,
    COUNT(sessions.id) session_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(sessions.device))) device_values
FROM customers
INNER JOIN sessions ON sessions.customer_id = customers.id
GROUP BY customers.id, customers.country
ORDER BY customers.country DESC;
