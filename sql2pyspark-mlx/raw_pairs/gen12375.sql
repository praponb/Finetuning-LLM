SELECT
    c.city,
    CASE WHEN c.credit_limit >= 16600 THEN 'high' WHEN c.credit_limit >= 2600 THEN 'mid' ELSE 'low' END credit_limit_bucket,
    MIN(s.started_at) min_started_at,
    COUNT(CASE WHEN s.device = 'mobile' THEN 1 END) mobile_count
FROM sessions s
RIGHT JOIN customers c ON s.customer_id = c.id
WHERE c.name LIKE '%Ivy%'
GROUP BY c.city, CASE WHEN c.credit_limit >= 16600 THEN 'high' WHEN c.credit_limit >= 2600 THEN 'mid' ELSE 'low' END
