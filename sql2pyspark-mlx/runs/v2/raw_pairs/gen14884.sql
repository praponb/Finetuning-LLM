SELECT CASE WHEN c.age >= 72 THEN 'high' WHEN c.age >= 37 THEN 'mid' ELSE 'low' END AS age_bucket, c.country, MIN(p.amount) AS min_amount
FROM payments p
JOIN orders o ON p.order_id = o.id
RIGHT OUTER JOIN customers c ON o.customer_id = c.id
WHERE p.method = 'card'
GROUP BY CASE WHEN c.age >= 72 THEN 'high' WHEN c.age >= 37 THEN 'mid' ELSE 'low' END, c.country
HAVING min_amount >= 740
ORDER BY age_bucket;
