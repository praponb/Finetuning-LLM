SELECT c.country, SUM(CASE WHEN s.device = 'mobile' THEN 1 ELSE 0 END) mobile_count
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id
GROUP BY c.country
ORDER BY c.country
