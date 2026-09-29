SELECT a.id, a.customer_id, a.opened_date, a.balance, c.email FROM accounts a LEFT JOIN customers c ON a.customer_id = c.id WHERE c.segment IN ('SMB', 'Corporate', 'Consumer') ORDER BY a.balance DESC
