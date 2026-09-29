WITH ranked_stores AS (
    SELECT
        id,
        region,
        sqft,
        store_name,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS rn
    FROM stores
    WHERE city = 'Tokyo'
)
SELECT
    id,
    region,
    sqft,
    store_name
FROM ranked_stores
WHERE rn <= 2;
