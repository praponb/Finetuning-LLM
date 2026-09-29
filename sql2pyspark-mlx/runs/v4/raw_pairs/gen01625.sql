WITH ValidStores AS (
    SELECT *, LOWER(city) AS city_lower
    FROM stores
    WHERE UPPER(store_name) LIKE '%IAM%'
),
stores_stats AS (
    SELECT city, MIN(opened_date) AS min_opened_date
    FROM ValidStores
    GROUP BY city
)
SELECT *
FROM stores_stats
