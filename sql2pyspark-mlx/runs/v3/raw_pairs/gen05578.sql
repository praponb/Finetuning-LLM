SELECT a.id, a.status, a.account_type, a.opened_date, c.segment, c.name FROM accounts a JOIN customers c ON c.id = a.customer_id ORDER BY status DESC
