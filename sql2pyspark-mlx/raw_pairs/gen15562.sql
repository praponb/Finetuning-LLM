SELECT t.txn_time, a.customer_id, c.country
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE t.is_flagged = FALSE
