WITH numbered AS (
    SELECT
        id,
        region,
        sqft,
        RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS rnk
    FROM stores
    WHERE store_name IS NULL
)
SELECT *
FROM numbered
WHERE rnk <= 2;
