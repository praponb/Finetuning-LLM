WITH ranked_stores AS (
    SELECT
        id,
        city,
        opened_date,
        region,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS pos
    FROM stores
)
SELECT
    id,
    city,
    opened_date,
    region
FROM ranked_stores
WHERE pos = 1
