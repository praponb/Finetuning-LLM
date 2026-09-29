SELECT id, supplier_id, tags FROM products WHERE SIZE(tags) >= 1 ORDER BY supplier_id DESC, 1;
