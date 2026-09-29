SELECT *
FROM (
    SELECT
        id,
        region,
        sqft,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS row_num
    FROM stores
    WHERE region NOT IN ('North', 'Central', 'South')
) AS t
WHERE row_num <= 5
