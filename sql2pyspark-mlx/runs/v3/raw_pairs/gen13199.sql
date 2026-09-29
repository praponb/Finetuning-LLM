SELECT p.id, p.brand, p.launch_date
FROM products p
WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE city IN ('Chiang Mai', 'Singapore'))
ORDER BY p.brand DESC, launch_date DESC
