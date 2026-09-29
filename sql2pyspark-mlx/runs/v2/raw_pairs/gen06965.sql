SELECT QUARTER(opened_date) AS opened_quarter, SUM(CASE WHEN region = 'Central' THEN 1 ELSE 0 END) AS central_count, MAX(opened_date) AS latest_opened_date
FROM stores
WHERE opened_date >= '2024-11-13'
GROUP BY QUARTER(opened_date)
ORDER BY opened_quarter
LIMIT 3
