SELECT c.is_active, COUNT(DISTINCT s.country) AS unique_countrys
FROM sessions s
FULL OUTER JOIN customers c ON s.customer_id = c.id
WHERE s.customer_id IN (18, 31)
GROUP BY c.is_active
ORDER BY unique_countrys DESC
