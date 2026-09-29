SELECT s.city, s.supplier_name, p.category FROM suppliers s INNER JOIN products p ON s.id = p.supplier_id WHERE UPPER(TRIM(p.product_name)) = 'NOISE-CANCEL HEADPHONES';
