SELECT
    id,
    launch_date,
    price,
    ROUND(AVG(price) OVER (ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS price_moving_avg_7
FROM products;
