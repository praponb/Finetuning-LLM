SELECT a.status, a.account_type, t.is_flagged, t.amount FROM accounts a INNER JOIN transactions t ON t.account_id = a.id WHERE MONTH(a.opened_date) IN (3, 10, 11)
