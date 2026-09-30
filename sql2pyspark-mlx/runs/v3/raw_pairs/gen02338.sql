SELECT a.status, t.account_id, t.amount, t.merchant FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id ORDER BY merchant DESC;
