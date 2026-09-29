WITH filtered_stores AS (
    SELECT *
    FROM stores
    WHERE region IN ('West', 'North') AND LENGTH(store_name) > 11
),
summary AS (
    SELECT city, MAX(sqft) AS max_sqft, COUNT(sqft) AS sqft_count
    FROM filtered_stores
    GROUP BY city
)
SELECT *
FROM summary;
