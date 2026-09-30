SELECT
    id,
    region,
    sqft,
    store_name
FROM (
    SELECT *, RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS pos
    FROM stores
) AS t
WHERE pos = 1
ORDER BY region DESC;
