SELECT a.id, a.opened_date, c.segment FROM accounts a JOIN customers c ON a.customer_id = c.id WHERE a.customer_id IS NULL AND a.opened_date BETWEEN '2023-03-01' AND '2024-03-01'
