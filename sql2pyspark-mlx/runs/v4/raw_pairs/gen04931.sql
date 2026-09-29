SELECT
    *,
    NTILE(5) OVER (PARTITION BY city ORDER BY sqft DESC, id) sqft_bucket,
    MIN(sqft) OVER (PARTITION BY region) region_min_sqft,
    LAG(sqft, 2) OVER (PARTITION BY region, city ORDER BY opened_date, id) prev_sqft,
    FIRST_VALUE(region, TRUE) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_region,
    DENSE_RANK() OVER (ORDER BY sqft DESC NULLS LAST) sqft_dense_rank
FROM stores;
