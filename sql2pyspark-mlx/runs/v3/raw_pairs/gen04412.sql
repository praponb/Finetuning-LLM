SELECT
    id,
    region,
    sqft,
    city,
    opened_date,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 3) AS sqft_share,
    ROUND(AVG(sqft) OVER (PARTITION BY region, city ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS sqft_moving_avg_3
FROM stores
