SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    FIRST_VALUE(region) IGNORE NULLS OVER w_part AS first_known_region,
    ROW_NUMBER() OVER w_hist AS row_seq,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
    NTILE(4) OVER w_hist AS sqft_bucket
FROM stores
WINDOW w_part AS (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY region ORDER BY sqft, id);
