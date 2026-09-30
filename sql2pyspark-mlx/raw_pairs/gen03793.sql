SELECT *, TRIM(status) AS clean_status, TRIM(status) AS clean_status_2
FROM subscriptions
WHERE NOT (plan IN ('premium', 'basic', 'standard') AND customer_id = 35 AND start_date < '2024-07-01')
