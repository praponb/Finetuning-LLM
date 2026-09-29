SELECT a.status, c.segment FROM accounts a INNER JOIN customers c ON c.id = a.customer_id ORDER BY a.status;
