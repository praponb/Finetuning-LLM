SELECT start_date, 'subscription' AS kind
FROM subscriptions
UNION
SELECT signup_date, 'customer' AS kind
FROM customers
ORDER BY kind DESC;
