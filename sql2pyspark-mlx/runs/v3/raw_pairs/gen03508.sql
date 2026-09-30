SELECT p.id, p.price, p.supplier_id, p.stock_qty FROM products p WHERE p.cost BETWEEN 330 AND 690 AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE city <> 'Singapore');
