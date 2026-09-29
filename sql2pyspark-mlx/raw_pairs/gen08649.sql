SELECT *, DATE_SUB(start_date, 43) AS start_date_minus_43d
FROM subscriptions
WHERE YEAR(start_date) = 2024 AND start_date < '2025-07-11';
