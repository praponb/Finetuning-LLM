SELECT p.id, p.launch_date FROM products p WHERE ARRAY_CONTAINS(p.tags, 'bestseller') AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE LENGTH(supplier_name) >= 10) ORDER BY id DESC
