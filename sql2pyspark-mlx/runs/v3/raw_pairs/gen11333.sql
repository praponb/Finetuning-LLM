SELECT c.country, COUNT(*) num_sessions, SUM(CASE WHEN s.country = 'US' THEN 1 ELSE 0 END) us_count
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
GROUP BY c.country
HAVING SUM(CASE WHEN s.country = 'US' THEN 1 ELSE 0 END) > 66
ORDER BY c.country DESC
