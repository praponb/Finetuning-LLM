SELECT oi.quantity, o.customer_id, o.order_date, COALESCE(o.shipping_fee, 0) AS shipping_fee_filled FROM order_items oi LEFT JOIN orders o ON oi.order_id = o.id ORDER BY customer_id;
