SELECT
    id,
    launch_date,
    brand,
    cost,
    ROUND(AVG(cost) OVER (PARTITION BY DATE_TRUNC('YEAR', launch_date), brand ORDER BY launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS cost_moving_avg_4
FROM products;
