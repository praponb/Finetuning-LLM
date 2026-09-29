SELECT MONTH(opened_date) opened_month, ROUND(STDDEV(sqft), 2) stddev_sqft, AVG(sqft) avg_sqft
FROM stores
WHERE region IS NOT NULL
GROUP BY opened_month;
