SELECT p.id, p.category, p.stock_qty FROM products p WHERE p.stock_qty != 259 AND NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.unit_price > 110)
