SELECT id, customer_id, plan
FROM subscriptions
WHERE MONTH(end_date) IN (4, 5, 7)
UNION ALL
SELECT id, customer_id, plan
FROM subscriptions
WHERE MONTH(end_date) IN (5, 9, 11)
ORDER BY id DESC;
