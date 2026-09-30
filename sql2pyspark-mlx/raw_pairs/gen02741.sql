SELECT balance, customer_id, CEIL(balance) AS balance_whole
FROM accounts
ORDER BY balance DESC, balance_whole DESC;
