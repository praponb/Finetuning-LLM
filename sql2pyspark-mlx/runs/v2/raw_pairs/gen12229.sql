SELECT c.city, COUNT(CASE WHEN s.device = 'desktop' THEN 1 END) AS desktop_count
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.name NOT LIKE '%Srisuk%'
GROUP BY c.city
ORDER BY desktop_count DESC, city
LIMIT 1
