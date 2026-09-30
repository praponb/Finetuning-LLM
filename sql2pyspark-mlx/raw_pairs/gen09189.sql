SELECT a.id, a.balance, a.status, a.customer_id FROM accounts a WHERE a.id IN (SELECT account_id FROM transactions) ORDER BY a.id ASC LIMIT 5;
