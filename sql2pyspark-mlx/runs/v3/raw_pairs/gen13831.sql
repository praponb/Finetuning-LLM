SELECT
    id,
    city,
    sqft,
    region,
    opened_date,
    store_name,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft DESC) AS sqft_dense_rank,
    FIRST_VALUE(store_name) OVER (PARTITION BY region ORDER BY opened_date, id) AS first_store_name
FROM stores
ORDER BY sqft;
