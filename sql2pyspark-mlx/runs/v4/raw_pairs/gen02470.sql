SELECT id, customer_id, order_date, shipping_fee, LEAD(shipping_fee, 2) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS next_shipping_fee FROM orders
