SELECT *, ROUND(amount * 100, 2) AS amount_adjusted, shipping_fee - amount AS amount_with_shipping FROM orders WHERE customer_id = 26;
