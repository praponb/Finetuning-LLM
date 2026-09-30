SELECT
    id,
    store_name,
    sqft,
    AVG(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS sqft_avg_2_prior,
    SUM(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share
FROM stores
WHERE UPPER(store_name) LIKE '%ALL%';
