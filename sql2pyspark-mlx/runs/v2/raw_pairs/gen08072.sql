SELECT id, category, cost, launch_date, ROUND(cost / SUM(cost) OVER (PARTITION BY category), 3) AS cost_share, DENSE_RANK() OVER (ORDER BY launch_date DESC) AS launch_date_dense_rank FROM products;
