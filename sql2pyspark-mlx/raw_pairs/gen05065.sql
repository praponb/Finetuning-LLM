SELECT id, supplier_id, cost, MIN(cost) OVER (PARTITION BY supplier_id) AS supplier_id_min_cost FROM products ORDER BY supplier_id DESC;
