SELECT
    id,
    region,
    sqft,
    store_name
FROM (
    SELECT
        id,
        region,
        sqft,
        store_name,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS pos
    FROM stores
) AS t
WHERE pos = 1
