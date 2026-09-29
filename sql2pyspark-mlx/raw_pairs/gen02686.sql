WITH top_stores AS (
    SELECT
        id,
        region,
        opened_date,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS row_num
    FROM stores
)
SELECT id, region, opened_date
FROM top_stores
WHERE row_num <= 5
