SELECT *, CASE WHEN country = 'SG' THEN 1 ELSE 0 END AS is_sg
FROM sessions
WHERE country != 'JP'
ORDER BY customer_id ASC, id ASC
LIMIT 5
