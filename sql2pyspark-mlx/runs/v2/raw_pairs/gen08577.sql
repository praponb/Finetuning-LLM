SELECT c.id, c.segment FROM customers c WHERE c.signup_date >= '2023-04-01' AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id) ORDER BY 2;
