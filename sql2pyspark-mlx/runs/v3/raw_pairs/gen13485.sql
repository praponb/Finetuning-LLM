SELECT status
FROM payments
WHERE method = 'promptpay'
EXCEPT
SELECT status
FROM subscriptions
