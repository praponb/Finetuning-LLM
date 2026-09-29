SELECT
    id,
    city,
    sqft,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft DESC) AS dense_rnk
FROM stores
ORDER BY dense_rnk
