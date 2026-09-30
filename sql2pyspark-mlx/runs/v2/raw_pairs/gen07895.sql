SELECT p.id, p.launch_date, p.category, p.stock_qty FROM products p WHERE ARRAY_CONTAINS(p.tags, 'clearance') AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE city IN ('Tokyo', 'Singapore'))
