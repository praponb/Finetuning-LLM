SELECT region, COUNT(DISTINCT sqft) unique_regions, SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END) south_count
FROM stores
GROUP BY region
ORDER BY unique_regions;
