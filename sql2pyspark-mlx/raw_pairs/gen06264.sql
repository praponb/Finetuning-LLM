SELECT c.country, c.name, a.balance, GREATEST(c.credit_limit, c.age) AS greatest_credit_limit_age FROM customers c LEFT OUTER JOIN accounts a ON a.customer_id = c.id ORDER BY a.balance DESC;
