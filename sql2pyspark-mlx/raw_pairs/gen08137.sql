SELECT status, plan, COUNT(monthly_fee) AS monthly_fee_count
FROM subscriptions
WHERE end_date < '2025-08-12' AND (YEAR(start_date) = 2023 OR MONTH(end_date) IN (3, 7, 10))
GROUP BY status, plan
HAVING SUM(monthly_fee) < 1330
ORDER BY plan;
