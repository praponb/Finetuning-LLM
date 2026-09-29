SELECT c.id, c.name, c.email, c.country FROM customers c WHERE c.signup_date >= '2024-08-25' AND EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id);
