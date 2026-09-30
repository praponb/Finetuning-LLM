SELECT o.id, o.amount, c.country FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.country = 'TH';
