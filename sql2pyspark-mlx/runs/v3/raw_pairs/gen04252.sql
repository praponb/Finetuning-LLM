WITH ranked AS (
    SELECT
        id,
        city,
        sqft,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS rnk
    FROM stores
)
SELECT id, city, sqft
FROM ranked
WHERE rnk <= 5
ORDER BY id DESC
