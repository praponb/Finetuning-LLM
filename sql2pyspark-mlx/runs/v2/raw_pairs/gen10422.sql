SELECT CASE WHEN c.age >= 72 THEN 'high' WHEN c.age >= 39 THEN 'mid' ELSE 'low' END AS age_bucket, MAX(s.monthly_fee) - MIN(s.monthly_fee) AS monthly_fee_range
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id
GROUP BY CASE WHEN c.age >= 72 THEN 'high' WHEN c.age >= 39 THEN 'mid' ELSE 'low' END
HAVING MAX(s.monthly_fee) - MIN(s.monthly_fee) >= 63
ORDER BY age_bucket DESC;
