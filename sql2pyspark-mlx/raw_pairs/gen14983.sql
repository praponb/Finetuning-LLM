SELECT
    *,
    NTILE(10) OVER w_ordered sqft_bucket,
    LAG(sqft, 2) OVER (PARTITION BY region, city ORDER BY sqft ASC, id ASC) prev_sqft,
    DENSE_RANK() OVER w_grp sqft_dense_rank
FROM stores
WHERE city IN ('Singapore', 'Berlin')
WINDOW w_ordered AS (PARTITION BY region ORDER BY sqft DESC, id ASC), w_grp AS (PARTITION BY region, city ORDER BY sqft ASC);
