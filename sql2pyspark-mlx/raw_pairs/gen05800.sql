SELECT id, supplier_id, cost, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY cost, id) AS cost_row FROM products WHERE cost <= 310
