SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, MAX(opened_date) AS last_opened_date
FROM stores
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
ORDER BY opened_month, last_opened_date
