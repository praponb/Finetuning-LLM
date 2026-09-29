SELECT i.id, i.quantity
FROM inventory i
WHERE i.quantity <> 161 AND i.product_id IN (SELECT id FROM products WHERE supplier_id IS NULL)
ORDER BY id;
