SELECT id, start_date, monthly_fee, plan, DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date, LENGTH(status) AS status_length FROM subscriptions ORDER BY days_since_end_date DESC;
