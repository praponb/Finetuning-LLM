SELECT *, SUBSTRING(plan, 1, 3) plan_prefix
FROM subscriptions
WHERE MONTH(start_date) IN (5, 7, 10) AND end_date > '2023-06-01';
