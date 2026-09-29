SELECT SUM(CASE WHEN category = 'Electronics' THEN stock_qty ELSE 0 END) AS electronics_stock_qty
FROM products
WHERE category = 'Electronics' OR supplier_id = 2
