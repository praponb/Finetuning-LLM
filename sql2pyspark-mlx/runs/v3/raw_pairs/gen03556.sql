SELECT
    c.id,
    c.segment,
    COUNT(s.id) AS session_count,
    COUNT(DISTINCT s.customer_id) AS unique_customers
FROM customers c
LEFT JOIN sessions s ON c.id = s.customer_id
GROUP BY c.id, c.segment
ORDER BY unique_customers DESC, id, segment
LIMIT 50;
