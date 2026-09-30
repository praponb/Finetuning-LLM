SELECT *
FROM (
    SELECT
        id,
        region,
        sqft,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft) AS rn
    FROM stores
    WHERE sqft != 8087
) AS r
WHERE rn <= 2;
