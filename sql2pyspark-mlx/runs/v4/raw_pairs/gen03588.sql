SELECT p.id, p.category, p.cost
FROM products p
WHERE p.category != 'Home' AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE supplier_name LIKE 'L%')
