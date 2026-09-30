SELECT s.id, s.city, s.rating
FROM suppliers s
WHERE s.id IN (SELECT supplier_id FROM products WHERE stock_qty <= 359);
