SELECT id, status, ADD_MONTHS(end_date, 3) AS end_date_plus_months
FROM subscriptions
WHERE customer_id IS NULL
ORDER BY status, end_date_plus_months DESC, id
LIMIT 10;
