SELECT p.id, p.product_name, p.supplier_id, p.cost FROM products p WHERE p.price < 370 AND p.supplier_id IN (SELECT id FROM suppliers WHERE country <> 'TH');
