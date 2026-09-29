SELECT MONTH(opened_date) AS opened_month, region, AVG(sqft) AS avg_sqft
FROM stores
GROUP BY MONTH(opened_date), region
ORDER BY 3 DESC;
