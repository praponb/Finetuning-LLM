SELECT a.opened_date, a.status, a.customer_id, c.segment, c.name FROM accounts a INNER JOIN customers c ON c.id = a.customer_id WHERE a.status IN ('OPEN', 'FROZEN') OR c.signup_date >= '2023-12-18';
