SELECT customer_id, DATEDIFF(CURRENT_DATE(), end_date) / 30 months_since_end_date, DATEDIFF(end_date, start_date) / 7 weeks_start_date_to_end_date
FROM subscriptions;
