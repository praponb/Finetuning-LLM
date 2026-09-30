SELECT p.stock_qty, p.launch_date, p.category, oi.unit_price, oi.order_id, oi.product_id FROM products p INNER JOIN order_items oi ON p.id = oi.product_id WHERE p.category IN ('Home', 'Books')
