SELECT id, supplier_id, launch_date, cost, ROUND(AVG(cost) OVER (PARTITION BY supplier_id ORDER BY launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS cost_moving_avg_4 FROM products
