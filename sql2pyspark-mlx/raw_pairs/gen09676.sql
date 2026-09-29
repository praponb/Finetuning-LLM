SELECT a.customer_id, a.status, t.is_flagged FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id WHERE t.merchant = 'Shell' ORDER BY status DESC;
