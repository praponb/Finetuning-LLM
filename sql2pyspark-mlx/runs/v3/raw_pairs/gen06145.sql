SELECT o.id, o.status, o.order_date, c.country, c.age FROM orders o INNER JOIN customers c ON c.id = o.customer_id ORDER BY o.status;
