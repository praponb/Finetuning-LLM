SELECT p.id, p.method, o.order_date FROM payments p INNER JOIN orders o ON p.order_id = o.id;
