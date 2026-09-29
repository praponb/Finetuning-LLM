SELECT
    id,
    launch_date,
    brand,
    price,
    ROUND(AVG(price) OVER (PARTITION BY DATE_TRUNC('WEEK', launch_date), brand ORDER BY launch_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS price_moving_avg_3
FROM products;
