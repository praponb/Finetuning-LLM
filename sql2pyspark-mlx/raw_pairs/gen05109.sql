SELECT *
FROM stores
WHERE MONTH(opened_date) IN (1, 8, 9)
ORDER BY region, sqft DESC
