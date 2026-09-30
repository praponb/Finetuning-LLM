SELECT o.id, o.amount, o.discount, o.store_id, c.country, c.name FROM orders o INNER JOIN customers c ON c.id = o.customer_id WHERE c.country = 'UK'
