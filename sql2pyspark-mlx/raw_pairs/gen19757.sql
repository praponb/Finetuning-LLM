SELECT
    id,
    city,
    region,
    opened_date,
    sqft,
    SUM(sqft) OVER w_running AS running_sum_sqft,
    AVG(sqft) OVER w_running AS running_avg_sqft,
    MAX(sqft) OVER w_running AS running_max_sqft
FROM stores
WHERE opened_date < '2025-04-18'
WINDOW w_running AS (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
