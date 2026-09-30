SELECT *, ROUND(monthly_fee, 0) AS monthly_fee_rounded, QUARTER(start_date) AS start_date_quarter
FROM subscriptions
WHERE status <> 'EXPIRED'
