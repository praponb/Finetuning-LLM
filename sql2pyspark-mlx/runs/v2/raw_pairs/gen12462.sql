SELECT id, supplier_id, launch_date, stock_qty, LEAD(stock_qty, 1, 0) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS next_stock_qty FROM products ORDER BY launch_date;
