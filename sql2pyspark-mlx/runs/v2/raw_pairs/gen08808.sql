SELECT *, MONTH(start_date) AS start_date_month, DATE_TRUNC('MONTH', start_date) AS start_date_period FROM subscriptions WHERE plan = 'premium' ORDER BY id DESC
