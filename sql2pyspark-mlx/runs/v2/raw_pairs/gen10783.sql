SELECT category, MONTH(launch_date) AS launch_month, MAX(cost) - MIN(cost) AS cost_range FROM products WHERE category IN ('Sports', 'Fashion', 'Electronics') GROUP BY category, launch_month;
