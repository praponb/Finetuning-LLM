SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    ROW_NUMBER() OVER w_part AS seq_no,
    CUME_DIST() OVER w_hist AS sqft_cume_dist,
    LAST_VALUE(region, TRUE) OVER (PARTITION BY region ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region,
    LAST_VALUE(sqft, TRUE) OVER (ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft
FROM stores
WINDOW w_part AS (PARTITION BY region ORDER BY sqft DESC, id ASC), w_hist AS (PARTITION BY region ORDER BY sqft DESC)
