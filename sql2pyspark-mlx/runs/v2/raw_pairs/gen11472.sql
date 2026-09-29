SELECT p.id, p.supplier_id, p.stock_qty FROM products p WHERE ARRAY_CONTAINS(p.tags, 'premium') AND p.id IN (SELECT product_id FROM store_sales WHERE product_id = 8) ORDER BY id DESC;
