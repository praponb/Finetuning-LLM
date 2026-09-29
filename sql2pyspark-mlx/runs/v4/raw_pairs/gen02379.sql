SELECT *
FROM (
    SELECT city, MAX(sqft) AS max_sqft
    FROM stores
    GROUP BY city
) AS agg
WHERE max_sqft > 7000
ORDER BY max_sqft DESC, city ASC
LIMIT 3
