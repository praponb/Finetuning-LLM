SELECT id, start_date, DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date
FROM subscriptions
WHERE customer_id IS NOT NULL AND monthly_fee < 150
ORDER BY start_date;
