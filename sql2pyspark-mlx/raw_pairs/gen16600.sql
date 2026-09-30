SELECT customer_id
FROM subscriptions
EXCEPT
SELECT customer_id
FROM subscriptions
WHERE monthly_fee IS NOT NULL;
