SELECT p.id, p.price, p.supplier_id
FROM products p
WHERE p.cost BETWEEN 470 AND 510 AND EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id);
