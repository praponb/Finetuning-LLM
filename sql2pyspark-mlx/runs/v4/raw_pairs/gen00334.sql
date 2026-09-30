SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    SUM(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sqft,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY region, city), 4) sqft_share,
    NTILE(3) OVER (ORDER BY sqft, id) sqft_bucket,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY region, city), 4) sqft_share_2
FROM stores;
