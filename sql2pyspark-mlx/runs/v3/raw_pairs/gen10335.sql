SELECT id, order_date, discount, discount - LAG(discount, 1) OVER (ORDER BY order_date, id) discount_change FROM orders ORDER BY id
