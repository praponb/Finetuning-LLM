SELECT p.id, p.supplier_id
FROM products p
WHERE p.supplier_id = 8 AND NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id)
ORDER BY supplier_id DESC
