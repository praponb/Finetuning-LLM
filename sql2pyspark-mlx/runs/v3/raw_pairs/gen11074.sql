SELECT *, LAST_DAY(start_date) AS start_date_month_end, YEAR(end_date) AS end_date_year FROM subscriptions WHERE monthly_fee <= 240
