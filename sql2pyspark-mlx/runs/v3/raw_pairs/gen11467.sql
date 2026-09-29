SELECT status, TRIM(status) AS clean_status, DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date FROM subscriptions ORDER BY days_since_start_date DESC, 1
