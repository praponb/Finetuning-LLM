SELECT p.id, p.cost, p.launch_date
FROM products p
WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating > 3.4);
