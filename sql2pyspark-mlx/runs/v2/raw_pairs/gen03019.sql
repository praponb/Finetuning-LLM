SELECT
    p.id,
    p.supplier_id,
    p.brand,
    p.price
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.product_id = 6)
ORDER BY p.supplier_id DESC, price DESC;
