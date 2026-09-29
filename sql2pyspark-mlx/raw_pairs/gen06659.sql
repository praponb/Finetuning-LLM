SELECT a.id, a.status, c.city, c.segment FROM accounts a INNER JOIN customers c ON c.id = a.customer_id WHERE a.balance <= 77000
