SELECT c.city, MAX(a.opened_date) last_opened_date, MIN(a.balance) min_balance FROM accounts a INNER JOIN customers c ON c.id = a.customer_id GROUP BY c.city;
