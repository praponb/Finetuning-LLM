SELECT region, SUM(sqft) AS total_sqft, ROUND(SUM(sqft) * 100.0 / SUM(SUM(sqft)) OVER (), 2) AS pct_of_total
FROM stores
WHERE region IN ('South', 'North')
GROUP BY region
ORDER BY region
LIMIT 10
