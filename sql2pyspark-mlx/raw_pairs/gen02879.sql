SELECT id, product_name, tags, supplier_id FROM products WHERE product_name IS NULL AND ARRAY_CONTAINS(tags, 'premium') AND cost <= 430;
