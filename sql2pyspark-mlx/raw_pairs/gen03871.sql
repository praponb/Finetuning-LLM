SELECT
    *,
    AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior,
    NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_date), city ORDER BY sqft DESC, id) AS sqft_bucket,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS row_seq
FROM stores
