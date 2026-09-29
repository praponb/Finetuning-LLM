SELECT oi.id, oi.unit_price, o.shipping_fee, o.customer_id, s.store_name, s.sqft FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id INNER JOIN stores s ON s.id = o.store_id
