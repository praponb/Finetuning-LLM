SELECT p.id, p.method, o.discount, o.amount, o.order_date FROM payments p INNER JOIN orders o ON p.order_id = o.id WHERE o.customer_id IS NULL AND p.method = 'card' ORDER BY discount DESC, id LIMIT 5
