SELECT status
FROM accounts
WHERE account_type != 'LOAN'
EXCEPT
SELECT status
FROM subscriptions
WHERE customer_id IN (13, 23);
