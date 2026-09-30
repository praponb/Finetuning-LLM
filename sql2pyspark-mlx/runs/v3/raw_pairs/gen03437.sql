SELECT o.id, o.channel, o.store_id, c.signup_date, c.name, c.segment FROM orders o LEFT JOIN customers c ON o.customer_id = c.id WHERE c.segment IN ('SMB', 'Corporate', 'Consumer');
