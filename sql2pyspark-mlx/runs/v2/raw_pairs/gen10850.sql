SELECT p.supplier_id, p.launch_date, p.cost, oi.product_id FROM products p LEFT OUTER JOIN order_items oi ON p.id = oi.product_id ORDER BY oi.product_id DESC
