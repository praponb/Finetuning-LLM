SELECT
    id,
    city,
    sqft,
    region,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS rn,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS rn_2
FROM stores
