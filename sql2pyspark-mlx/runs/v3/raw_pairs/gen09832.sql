SELECT p.id, p.category
FROM products p
WHERE p.category IN ('Sports', 'Electronics', 'Books') AND p.supplier_id IN (SELECT id FROM suppliers WHERE city IN ('Singapore', 'Bangkok'))
