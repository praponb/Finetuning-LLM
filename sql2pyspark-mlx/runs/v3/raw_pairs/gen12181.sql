SELECT id, category, supplier_id, brand FROM products WHERE supplier_id IS NULL OR price IS NOT NULL
