SELECT
    id,
    region,
    sqft,
    opened_date,
    DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS sqft_dense_rank,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS row_num
FROM stores
