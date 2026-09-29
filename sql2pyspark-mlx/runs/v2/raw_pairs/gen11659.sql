WITH ranked AS (
    SELECT
        id,
        city,
        sqft,
        store_name,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft) AS rn
    FROM stores
)
SELECT
    id,
    city,
    sqft,
    store_name
FROM ranked
WHERE rn <= 3;
