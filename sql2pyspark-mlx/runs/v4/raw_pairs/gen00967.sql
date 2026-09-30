SELECT c.age, a.account_type, a.customer_id
FROM customers c
LEFT JOIN accounts a ON a.customer_id = c.id;
