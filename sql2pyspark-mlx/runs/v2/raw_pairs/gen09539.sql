SELECT region, COUNT(*) AS cnt, COUNT(DISTINCT sqft) AS unique_citys
FROM stores
GROUP BY 1
HAVING COUNT(DISTINCT sqft) > 4
ORDER BY cnt ASC;
