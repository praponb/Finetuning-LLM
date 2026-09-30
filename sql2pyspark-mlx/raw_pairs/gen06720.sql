SELECT id, start_date, DATE_TRUNC('WEEK', start_date) start_date_period FROM subscriptions WHERE monthly_fee >= 80;
