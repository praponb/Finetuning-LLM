SELECT *, DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date, SUBSTRING(status, 1, 4) AS status_prefix FROM subscriptions ORDER BY customer_id DESC;
