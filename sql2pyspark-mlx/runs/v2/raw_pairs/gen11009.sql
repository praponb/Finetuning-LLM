SELECT id, supplier_id, launch_date, cost, SUM(cost) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS running_cost FROM products WHERE supplier_id = 5;
