SELECT
    id,
    region,
    sqft,
    city,
    opened_date,
    RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS sqft_rank,
    SUM(sqft) OVER (PARTITION BY region, city ORDER BY opened_date, id) AS running_sqft
FROM stores
WHERE store_name LIKE 'Do%';
