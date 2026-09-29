SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id;
