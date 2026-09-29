SELECT product_name, cost, COALESCE(stock_qty, 0) AS stock_qty_filled FROM products WHERE brand IN ('Initech', 'Stark') ORDER BY cost DESC;
