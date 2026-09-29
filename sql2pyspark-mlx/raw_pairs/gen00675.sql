SELECT *, CEIL(monthly_fee) AS monthly_fee_whole, DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
FROM subscriptions
WHERE status != 'CANCELLED' OR monthly_fee IS NOT NULL;
