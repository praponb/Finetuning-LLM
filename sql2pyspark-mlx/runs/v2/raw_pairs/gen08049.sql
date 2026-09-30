SELECT a.customer_id, a.account_type, t.merchant, t.amount FROM accounts a LEFT OUTER JOIN transactions t ON a.id = t.account_id WHERE a.customer_id IN (15, 27, 35) ORDER BY t.amount DESC;
