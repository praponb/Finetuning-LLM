SELECT s.* FROM suppliers s WHERE NOT EXISTS (SELECT 1 FROM products ch WHERE ch.supplier_id = s.id AND ch.brand IN ('Initech', 'Umbrella'));
