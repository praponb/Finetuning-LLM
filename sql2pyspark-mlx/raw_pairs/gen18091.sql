SELECT
    c.id,
    c.city,
    COUNT(s.id) AS subscription_count,
    COUNT(CASE WHEN s.status = 'PAUSED' THEN 1 END) AS paused_count
FROM customers c
LEFT JOIN subscriptions s ON c.id = s.customer_id
WHERE c.country != 'DE'
GROUP BY c.id, c.city
ORDER BY city, id
LIMIT 10;
