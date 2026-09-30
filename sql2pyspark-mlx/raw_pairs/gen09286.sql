SELECT p.id, p.stock_qty, p.cost
FROM products p
WHERE p.id IN (SELECT product_id FROM inventory WHERE last_restock < '2025-10-01');
