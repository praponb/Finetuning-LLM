SELECT id, region, opened_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS pos
    FROM stores
) AS r
WHERE pos <= 2
