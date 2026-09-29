SELECT c.city, COUNT(DISTINCT s.country) unique_countrys
FROM sessions s
RIGHT JOIN customers c ON c.id = s.customer_id
WHERE c.country != 'DE'
GROUP BY c.city
HAVING COUNT(DISTINCT s.country) >= 5
