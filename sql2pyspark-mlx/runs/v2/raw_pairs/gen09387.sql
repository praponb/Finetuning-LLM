SELECT
    c.id,
    c.country,
    COUNT(s.id) subscription_count,
    SUM(CASE WHEN s.plan = 'basic' THEN 1 ELSE 0 END) basic_count,
    MIN(s.end_date) first_end_date
FROM customers c
LEFT JOIN subscriptions s ON c.id = s.customer_id
GROUP BY c.id, c.country
ORDER BY id, country
LIMIT 1;
