SELECT *, DATEDIFF(start_date, end_date) days_end_date_to_start_date, DATEDIFF(start_date, end_date) / 7 weeks_end_date_to_start_date
FROM subscriptions
WHERE monthly_fee > 100
