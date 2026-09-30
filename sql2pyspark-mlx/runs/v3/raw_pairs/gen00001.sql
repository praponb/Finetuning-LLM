SELECT
    p.id,
    p.brand,
    p.supplier_id,
    p.product_name
FROM products p
WHERE EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.quantity BETWEEN 156 AND 187);
