SELECT p.id, p.price, s.country
FROM products p
LEFT JOIN suppliers s ON s.id = p.supplier_id
WHERE s.city = 'Berlin'
