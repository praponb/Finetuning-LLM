SELECT region, DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, SUM(CASE WHEN region = 'Central' THEN 1 ELSE 0 END) AS central_count
FROM stores
WHERE city NOT IN ('Tokyo', 'Singapore')
GROUP BY 1, 2
HAVING SUM(CASE WHEN region = 'Central' THEN 1 ELSE 0 END) >= 68
ORDER BY region, opened_month
LIMIT 5
