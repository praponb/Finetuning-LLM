SELECT region, city, sqft_count
FROM (
    SELECT region, city, COUNT(sqft) AS sqft_count
    FROM stores
    WHERE opened_date BETWEEN '2024-10-17' AND '2025-09-21'
    GROUP BY region, city
) AS x
WHERE sqft_count > 6
ORDER BY sqft_count DESC, region, city
LIMIT 10
