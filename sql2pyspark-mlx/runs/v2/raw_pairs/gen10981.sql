SELECT id, customer_id, DATEDIFF(CURRENT_DATE(), end_date) days_since_end_date FROM subscriptions WHERE end_date > '2025-06-10';
