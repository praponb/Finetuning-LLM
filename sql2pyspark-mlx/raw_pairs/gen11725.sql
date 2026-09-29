SELECT id, price, supplier_id, ARRAY_CONTAINS(tags, 'premium') is_premium FROM products WHERE SIZE(tags) > 1
