SELECT discount, shipping_fee - amount AS amount_with_shipping FROM orders WHERE MONTH(order_date) IN (1, 10, 12);
