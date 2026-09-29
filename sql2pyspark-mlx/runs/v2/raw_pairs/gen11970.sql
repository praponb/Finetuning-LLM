SELECT id, supplier_id, launch_date, cost, LEAD(cost, 1) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS next_cost FROM products WHERE price >= 810;
