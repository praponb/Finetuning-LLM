SELECT id, region, opened_date
FROM (
    SELECT
        id,
        region,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS pos
    FROM stores
    WHERE opened_date > '2023-11-01'
) AS ranked
WHERE pos = 1
ORDER BY region
