SELECT p.id, p.cost, p.stock_qty, p.supplier_id FROM products p WHERE EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.last_restock < '2025-08-28') ORDER BY id
