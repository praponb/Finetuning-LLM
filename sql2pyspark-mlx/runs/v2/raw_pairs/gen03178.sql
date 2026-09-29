SELECT c.id, c.country, c.email FROM customers c WHERE c.credit_limit <= 12900 AND c.id IN (SELECT customer_id FROM accounts);
