SELECT a.balance, a.opened_date, a.status, c.city FROM accounts a LEFT JOIN customers c ON a.customer_id = c.id WHERE a.customer_id IS NULL ORDER BY a.opened_date;
