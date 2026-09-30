SELECT id, monthly_fee, ADD_MONTHS(start_date, 6) start_date_plus_months
FROM subscriptions
WHERE status = 'PAUSED' OR customer_id IS NULL;
