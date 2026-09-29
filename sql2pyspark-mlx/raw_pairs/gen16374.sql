SELECT
    id,
    city,
    opened_date,
    sqft,
    AVG(sqft) OVER w_part AS sqft_avg_5_prior,
    LAG(sqft, 1) OVER w_running AS prev_sqft
FROM stores
WINDOW w_part AS (PARTITION BY region, city ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_running AS (ORDER BY sqft, id);
