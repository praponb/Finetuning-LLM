SELECT *, TRIM(plan) AS clean_plan, DATE_SUB(start_date, 1) AS start_date_minus_1d
FROM subscriptions
WHERE start_date < '2023-10-01' OR MONTH(end_date) IN (6, 10, 12)
