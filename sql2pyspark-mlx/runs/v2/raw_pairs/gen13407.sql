WITH numbered AS (
    SELECT
        id,
        city,
        opened_date,
        region,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS rn
    FROM stores
)
SELECT
    id,
    city,
    opened_date,
    region
FROM numbered
WHERE rn <= 3
