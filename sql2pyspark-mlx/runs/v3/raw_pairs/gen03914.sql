WITH top_stores AS (
    SELECT
        id,
        city,
        sqft,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS row_num
    FROM stores
)
SELECT *
FROM top_stores
WHERE row_num <= 1
ORDER BY row_num, id DESC
