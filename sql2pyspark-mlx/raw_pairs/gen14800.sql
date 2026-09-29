SELECT id, customer_id
FROM subscriptions
WHERE monthly_fee > 210 OR customer_id IN (7, 24, 26, 30)
ORDER BY id;
