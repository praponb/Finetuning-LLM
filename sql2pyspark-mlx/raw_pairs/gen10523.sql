SELECT *, DATE_TRUNC('MONTH', start_date) AS start_date_period FROM subscriptions WHERE start_date BETWEEN '2023-06-01' AND '2025-01-01' ORDER BY id DESC LIMIT 3;
