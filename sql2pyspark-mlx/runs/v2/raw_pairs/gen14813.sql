SELECT p.price, p.cost, s.country
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
ORDER BY s.country DESC
