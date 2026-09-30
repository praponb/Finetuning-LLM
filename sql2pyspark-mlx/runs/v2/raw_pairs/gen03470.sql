SELECT p.category, s.rating
FROM products p
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.supplier_id IS NULL AND p.stock_qty < 237
ORDER BY rating DESC, p.category DESC;
