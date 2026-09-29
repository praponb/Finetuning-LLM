SELECT id, launch_date, cost, LEAD(cost, 1) OVER (ORDER BY launch_date, id) next_cost FROM products WHERE supplier_id IS NULL
