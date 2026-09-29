SELECT segment, SUM(CASE WHEN country = 'TH' THEN age ELSE 0 END) AS th_age
FROM customers
WHERE LOWER(email) LIKE '%vy@%'
GROUP BY segment
HAVING SUM(CASE WHEN country = 'TH' THEN age ELSE 0 END) > 39
ORDER BY segment;
