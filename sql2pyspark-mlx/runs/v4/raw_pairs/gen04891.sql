SELECT *, UPPER(plan) AS plan_upper, DATE_ADD(start_date, 31) AS start_date_plus_31d FROM subscriptions WHERE end_date < '2023-05-17' AND (customer_id = 32 OR YEAR(start_date) = 2025)
