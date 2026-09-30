SELECT region, COUNT(*) AS num_stores, SUM(CASE WHEN region = 'Central' THEN sqft ELSE 0 END) AS central_sqft
FROM stores
GROUP BY region
HAVING COUNT(*) >= 1
ORDER BY num_stores DESC, region ASC
LIMIT 5;
