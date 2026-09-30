SELECT
    id,
    region,
    opened_date,
    store_name
FROM (
    SELECT *, RANK() OVER (PARTITION BY region ORDER BY opened_date) AS pos
    FROM stores
    WHERE LENGTH(store_name) < 5
) AS r
WHERE pos <= 3
