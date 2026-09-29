SELECT o.store_id, o.channel, o.status, c.age FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.segment = 'SMB' AND c.signup_date < '2024-03-01';
