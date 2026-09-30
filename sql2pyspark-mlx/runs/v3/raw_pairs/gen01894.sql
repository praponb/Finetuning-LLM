SELECT CASE WHEN c.age >= 67 THEN 'high' WHEN c.age >= 26 THEN 'mid' ELSE 'low' END AS age_bucket, c.is_active, COUNT(DISTINCT a.balance) AS unique_statuss
FROM accounts a
RIGHT JOIN customers c ON a.customer_id = c.id
WHERE LOWER(c.email) LIKE '%any%'
GROUP BY CASE WHEN c.age >= 67 THEN 'high' WHEN c.age >= 26 THEN 'mid' ELSE 'low' END, c.is_active
ORDER BY unique_statuss ASC
