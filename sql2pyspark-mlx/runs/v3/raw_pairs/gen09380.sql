SELECT *, ADD_MONTHS(end_date, 6) AS end_date_plus_months
FROM subscriptions
ORDER BY end_date;
