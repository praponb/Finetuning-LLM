SELECT customer_id, discount - amount AS discount_minus_amount, DATE_ADD(order_date, 60) AS order_date_plus_60d FROM orders WHERE channel NOT IN ('web', 'mobile', 'store')
