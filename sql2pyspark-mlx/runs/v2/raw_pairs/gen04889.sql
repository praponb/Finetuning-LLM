WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS pos
    FROM stores
    WHERE region = 'Central'
)
SELECT
    id,
    region,
    opened_date,
    store_name
FROM Numbered
WHERE pos <= 3
