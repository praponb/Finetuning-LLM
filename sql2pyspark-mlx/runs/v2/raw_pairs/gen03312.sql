SELECT c.id, c.email, c.country FROM customers c WHERE c.name LIKE '%Jac%' AND EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND YEAR(a.opened_date) = 2023) ORDER BY c.email;
