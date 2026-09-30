SELECT end_date, status, INITCAP(TRIM(status)) AS status_title, DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date FROM subscriptions WHERE customer_id IS NOT NULL;
