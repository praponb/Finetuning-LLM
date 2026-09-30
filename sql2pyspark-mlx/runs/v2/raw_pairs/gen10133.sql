WITH filtered_stores AS (
    SELECT *
    FROM stores
    WHERE region = 'East'
),
summary AS (
    SELECT city, COUNT(DISTINCT sqft) AS unique_citys
    FROM filtered_stores
    GROUP BY city
)
SELECT city, unique_citys
FROM summary
