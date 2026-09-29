SELECT
    id,
    brand,
    category,
    launch_date,
    cost,
    ROUND(AVG(cost) OVER (PARTITION BY brand, category ORDER BY launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) cost_moving_avg_4
FROM products;
