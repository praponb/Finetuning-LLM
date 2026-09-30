SELECT end_date AS event_date
FROM subscriptions
INTERSECT
SELECT signup_date AS event_date
FROM customers
