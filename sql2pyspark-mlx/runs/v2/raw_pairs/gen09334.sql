SELECT p.id, p.stock_qty, p.cost
FROM products p
WHERE p.id IN (SELECT product_id FROM store_sales WHERE revenue >= 2000);
