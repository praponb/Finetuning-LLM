SELECT id, monthly_fee, end_date, FIRST_VALUE(end_date) OVER (ORDER BY monthly_fee, id) AS first_end_date FROM subscriptions WHERE end_date > '2025-03-16';
