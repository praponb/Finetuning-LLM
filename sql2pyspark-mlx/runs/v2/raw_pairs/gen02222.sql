SELECT
    id,
    region,
    sqft,
    opened_date
FROM (
    SELECT
        id,
        region,
        sqft,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS pos
    FROM stores
) AS r
WHERE pos <= 2
ORDER BY sqft DESC
