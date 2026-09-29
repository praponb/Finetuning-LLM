SELECT *, COALESCE(stock_qty, 0) AS stock_qty_filled
FROM products
WHERE brand IN ('Stark', 'Acme') OR supplier_id IS NOT NULL
