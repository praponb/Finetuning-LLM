SELECT p.id, p.brand FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE LOWER(supplier_name) LIKE '%iam%') ORDER BY 1 DESC LIMIT 10;
