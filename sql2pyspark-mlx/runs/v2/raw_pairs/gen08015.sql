SELECT id, order_date, shipping_fee, LAG(shipping_fee, 1) OVER (ORDER BY order_date, id) prev_shipping_fee FROM orders WHERE shipping_fee < 19
