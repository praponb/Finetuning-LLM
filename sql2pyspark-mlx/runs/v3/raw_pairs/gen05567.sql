SELECT id, launch_date, cost, SUM(cost) OVER (ORDER BY launch_date, id) AS running_cost FROM products ORDER BY cost DESC;
