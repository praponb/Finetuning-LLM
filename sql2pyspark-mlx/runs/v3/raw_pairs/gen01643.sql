SELECT region, SUM(sqft) AS total_sqft, SUM(SUM(sqft)) OVER (ORDER BY region) AS cumulative_sqft
FROM stores
WHERE store_name NOT LIKE '%Airpor%'
GROUP BY region
ORDER BY 3 DESC, total_sqft DESC;
