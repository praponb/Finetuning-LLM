SELECT c.segment, c.name, c.email, a.status FROM customers c LEFT JOIN accounts a ON c.id = a.customer_id;
