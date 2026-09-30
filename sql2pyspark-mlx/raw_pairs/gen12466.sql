SELECT
    p.id,
    p.stock_qty,
    p.supplier_id,
    p.launch_date
FROM products p
WHERE p.product_name LIKE 'Ru%' AND EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.store_id = 3)
