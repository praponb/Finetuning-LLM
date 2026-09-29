WITH ranked AS (
    SELECT
        id,
        region,
        sqft,
        store_name,
        RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS rn
    FROM stores
)
SELECT
    id,
    region,
    sqft,
    store_name
FROM ranked
WHERE rn <= 1
