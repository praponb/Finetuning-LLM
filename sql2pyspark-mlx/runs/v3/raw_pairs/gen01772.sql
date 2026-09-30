SELECT s.end_date, c.credit_limit FROM subscriptions s RIGHT JOIN customers c ON s.customer_id = c.id WHERE LENGTH(c.name) < 14 OR s.end_date BETWEEN '2024-02-01' AND '2024-10-01'
