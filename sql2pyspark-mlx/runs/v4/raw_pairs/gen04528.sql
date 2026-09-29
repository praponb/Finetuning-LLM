SELECT c.id, c.credit_limit FROM customers c WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND YEAR(a.opened_date) = 2023) ORDER BY credit_limit;
