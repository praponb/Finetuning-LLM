SELECT
    id,
    opened_date,
    sqft,
    ROUND(AVG(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS sqft_moving_avg_3
FROM stores
ORDER BY id DESC, opened_date
