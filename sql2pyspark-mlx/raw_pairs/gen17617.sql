SELECT end_date, status FROM subscriptions WHERE YEAR(start_date) >= 2025 EXCEPT SELECT end_date, status FROM subscriptions WHERE end_date >= '2023-08-01';
