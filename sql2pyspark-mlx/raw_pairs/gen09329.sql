WITH valid_stores AS (
    SELECT *
    FROM stores
    WHERE city = 'Bangkok' AND store_name IS NOT NULL AND opened_date > '2024-02-09'
),
city_summary AS (
    SELECT city, ROUND(STDDEV(sqft), 2) stddev_sqft
    FROM valid_stores
    GROUP BY city
)
SELECT city, stddev_sqft
FROM city_summary
WHERE stddev_sqft < 27
ORDER BY stddev_sqft DESC
