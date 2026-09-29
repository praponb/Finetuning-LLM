SELECT
    id,
    region,
    city,
    sqft,
    ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft DESC, id) AS rn
FROM stores;
