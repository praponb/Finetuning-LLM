SELECT monthly_fee, 'subscription' AS source
FROM subscriptions
UNION
SELECT amount, 'transaction' AS source
FROM transactions;
