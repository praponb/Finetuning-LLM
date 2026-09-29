SELECT a.customer_id, t.amount FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id ORDER BY t.amount;
