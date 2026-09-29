SELECT p.id, p.product_name, p.cost, p.brand FROM products p WHERE p.brand IN ('Stark', 'Umbrella') AND p.supplier_id IN (SELECT id FROM suppliers WHERE LENGTH(supplier_name) >= 6)
