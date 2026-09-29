SELECT a.customer_id, t.merchant FROM accounts a INNER JOIN transactions t ON a.id = t.account_id WHERE a.status = 'OPEN' ORDER BY merchant ASC, a.customer_id DESC;
