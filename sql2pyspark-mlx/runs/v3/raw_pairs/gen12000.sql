SELECT region, COUNT(sqft) AS sqft_count, COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count
FROM stores
WHERE sqft > 9706
GROUP BY region
ORDER BY sqft_count DESC, region
LIMIT 10
