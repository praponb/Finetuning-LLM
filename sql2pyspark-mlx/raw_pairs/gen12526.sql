SELECT order_date, customer_id, ROUND(discount * 1.1, 2) adjusted_discount FROM orders WHERE shipping_fee > 33 ORDER BY customer_id ASC, order_date DESC
