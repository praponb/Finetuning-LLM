SELECT
    id,
    region,
    opened_date,
    city
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS pos
    FROM stores
) AS t
WHERE pos <= 2
