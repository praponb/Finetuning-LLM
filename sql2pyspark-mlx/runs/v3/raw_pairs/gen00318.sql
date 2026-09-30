SELECT a.id, a.customer_id, c.city, c.email FROM accounts a INNER JOIN customers c ON c.id = a.customer_id WHERE c.country = 'TH' AND a.status != 'OPEN';
