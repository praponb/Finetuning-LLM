SELECT a.customer_id, a.opened_date, t.amount, UPPER(a.status) AS status_upper FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id WHERE t.is_flagged = TRUE ORDER BY status_upper
