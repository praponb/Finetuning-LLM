SELECT o.status, oi.product_id, oi.unit_price FROM orders o INNER JOIN order_items oi ON o.id = oi.order_id WHERE o.store_id = 10 OR o.status = 'RETURNED';
