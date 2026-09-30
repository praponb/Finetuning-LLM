SELECT a.account_type, a.status, t.amount FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id WHERE a.opened_date BETWEEN '2023-06-11' AND '2024-10-01'
