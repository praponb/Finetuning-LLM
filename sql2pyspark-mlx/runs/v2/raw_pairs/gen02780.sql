SELECT *
FROM (
    SELECT city, COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) chiang_mai_count, COUNT(sqft) sqft_count
    FROM stores
    GROUP BY city
) x
WHERE sqft_count > 9
