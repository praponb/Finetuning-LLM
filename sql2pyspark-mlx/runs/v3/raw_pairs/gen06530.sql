SELECT city, COUNT(sqft) AS sqft_count, COUNT(*) AS cnt
FROM stores
GROUP BY city
HAVING COUNT(sqft) >= 4
