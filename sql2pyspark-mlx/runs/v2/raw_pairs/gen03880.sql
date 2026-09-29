SELECT p.amount, o.status, c.name, c.country FROM payments p INNER JOIN orders o ON p.order_id = o.id INNER JOIN customers c ON c.id = o.customer_id ORDER BY country DESC, amount DESC;
