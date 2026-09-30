WITH numbered AS (
    SELECT
        id,
        city,
        sqft,
        region,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS rn
    FROM stores
)
SELECT *
FROM numbered
WHERE rn <= 3
ORDER BY region DESC
