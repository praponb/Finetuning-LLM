WITH ranked AS (
    SELECT
        id,
        region,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS rn
    FROM stores
)
SELECT id, region, opened_date
FROM ranked
WHERE rn <= 3
ORDER BY id DESC
