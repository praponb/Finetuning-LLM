SELECT city, min_opened_date, min_sqft
FROM (
    SELECT city, MIN(opened_date) AS min_opened_date, MIN(sqft) AS min_sqft
    FROM stores
    WHERE region IN ('Central', 'West')
    GROUP BY city
) AS t
WHERE min_sqft > 14200
ORDER BY min_sqft DESC
