SELECT p.id, p.supplier_id FROM products p WHERE ARRAY_CONTAINS(p.tags, 'sale') AND NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.quantity <> 4)
