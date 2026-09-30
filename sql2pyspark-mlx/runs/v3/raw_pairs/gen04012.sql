SELECT id, end_date, plan, monthly_fee, DATE_TRUNC('YEAR', end_date) AS end_date_period FROM subscriptions;
