SELECT p.amount, o.amount AS order_amount, o.store_id FROM payments p INNER JOIN orders o ON p.order_id = o.id;
