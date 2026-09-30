SELECT id, region, sqft
FROM (
    SELECT
        id,
        region,
        sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS row_num
    FROM stores
) AS t
WHERE row_num <= 3
ORDER BY 2
