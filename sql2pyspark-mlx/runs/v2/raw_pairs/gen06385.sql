SELECT id, tags, brand, stock_qty, SIZE(tags) tags_count FROM products WHERE cost <= 320 OR ARRAY_CONTAINS(tags, 'sale');
