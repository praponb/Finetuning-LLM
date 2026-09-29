SELECT id, region, sqft
FROM (
    SELECT
        id,
        region,
        sqft,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft) AS row_num
    FROM stores
) AS r
WHERE row_num = 1
ORDER BY region;
