SELECT
    id,
    category,
    launch_date,
    price,
    brand,
    cost,
    ROUND(AVG(price) OVER (PARTITION BY category ORDER BY launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS price_moving_avg_4,
    ROUND(cost / SUM(cost) OVER (PARTITION BY brand), 3) AS cost_share
FROM products
WHERE supplier_id = 25
