SELECT
    *,
    LEAD(sqft) OVER (PARTITION BY city, region ORDER BY sqft, id) AS next_sqft,
    LAG(sqft) OVER (PARTITION BY city, region ORDER BY sqft, id) AS prev_sqft,
    ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft, id) AS seq_no,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share
FROM stores
