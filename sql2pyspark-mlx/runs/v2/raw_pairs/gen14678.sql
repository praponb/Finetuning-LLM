WITH ranked_stores AS (
    SELECT
        id,
        region,
        opened_date,
        sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS row_num
    FROM stores
)
SELECT
    id,
    region,
    opened_date,
    sqft
FROM ranked_stores
WHERE row_num = 1
ORDER BY region;
