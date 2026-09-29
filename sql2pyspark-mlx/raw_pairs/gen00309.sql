SELECT *, DATEDIFF(end_date, start_date) / 7 weeks_start_date_to_end_date
FROM subscriptions
WHERE monthly_fee <= 140 AND plan != 'premium'
ORDER BY weeks_start_date_to_end_date, id
LIMIT 3
