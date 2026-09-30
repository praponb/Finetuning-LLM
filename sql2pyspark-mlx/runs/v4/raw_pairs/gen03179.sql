SELECT s.*, o.discount, o.order_date, o.amount AS order_amount FROM payments s LEFT JOIN orders o ON s.order_id = o.id
