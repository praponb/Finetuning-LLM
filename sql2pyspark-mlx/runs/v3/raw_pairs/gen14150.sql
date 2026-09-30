SELECT city, region, COUNT(DISTINCT sqft) AS unique_regions
FROM stores
WHERE sqft <= 4673 AND sqft <= 14640
GROUP BY city, region
HAVING COUNT(DISTINCT sqft) > 4
ORDER BY region DESC;
