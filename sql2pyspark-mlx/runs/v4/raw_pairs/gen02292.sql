SELECT MONTH(opened_date) AS opened_month, MAX(sqft) AS max_sqft
FROM stores
WHERE region IS NOT NULL
GROUP BY MONTH(opened_date)
