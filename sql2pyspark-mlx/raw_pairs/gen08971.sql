SELECT p.id, p.cost, p.launch_date FROM products p WHERE p.category IN ('Electronics', 'Books') AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE city = 'Berlin') ORDER BY p.id DESC, cost ASC
