SELECT MONTH(opened_date) opened_month, SUM(sqft) total_sqft
FROM stores
GROUP BY MONTH(opened_date)
ORDER BY 2 DESC
