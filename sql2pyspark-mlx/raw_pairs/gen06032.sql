SELECT t.id, t.is_flagged, a.customer_id, a.opened_date, a.balance FROM transactions t LEFT OUTER JOIN accounts a ON a.id = t.account_id
