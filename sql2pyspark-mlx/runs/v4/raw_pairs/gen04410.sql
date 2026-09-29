SELECT
    c.id,
    c.name,
    COUNT(s.id) session_count,
    MIN(s.started_at) first_started_at
FROM customers c
INNER JOIN sessions s ON c.id = s.customer_id
WHERE s.customer_id = 34
GROUP BY c.id, c.name
ORDER BY first_started_at DESC
