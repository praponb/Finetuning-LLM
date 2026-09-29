SELECT balance, ROUND(balance, 0) balance_rounded
FROM accounts
WHERE customer_id IN (5, 12, 19, 22)
