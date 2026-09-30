WITH ranked_stores AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS pos
    FROM stores
)
SELECT
    id,
    city,
    sqft,
    store_name
FROM ranked_stores
WHERE pos <= 1
