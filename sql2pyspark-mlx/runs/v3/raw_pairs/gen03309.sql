SELECT CASE WHEN c.age >= 71 THEN 'high' WHEN c.age >= 36 THEN 'mid' ELSE 'low' END AS age_bucket, COUNT(CASE WHEN p.method = 'card' THEN 1 END) AS card_count
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY CASE WHEN c.age >= 71 THEN 'high' WHEN c.age >= 36 THEN 'mid' ELSE 'low' END
HAVING COUNT(CASE WHEN p.method = 'card' THEN 1 END) > 3
