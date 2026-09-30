SELECT status, DATEDIFF(end_date, start_date) AS days_start_date_to_end_date, DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label
FROM subscriptions
WHERE customer_id IS NOT NULL;
