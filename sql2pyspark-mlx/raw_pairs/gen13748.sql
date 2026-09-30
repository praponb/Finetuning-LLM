SELECT brand, COUNT(*) AS product_count FROM products WHERE cost IS NULL AND (ARRAY_CONTAINS(tags, 'eco') OR supplier_id = 13) GROUP BY brand
