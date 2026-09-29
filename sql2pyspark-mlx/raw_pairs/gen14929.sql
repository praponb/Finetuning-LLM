SELECT id, customer_id, ROUND(balance * 100, 2) AS adjusted_balance
FROM accounts;
