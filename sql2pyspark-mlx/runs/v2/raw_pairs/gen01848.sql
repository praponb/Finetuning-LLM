SELECT a.account_type, c.email, c.country FROM accounts a JOIN customers c ON a.customer_id = c.id WHERE a.status = 'CLOSED' AND a.opened_date <= '2025-06-16'
