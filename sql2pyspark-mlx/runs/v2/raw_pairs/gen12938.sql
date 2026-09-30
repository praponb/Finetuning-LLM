SELECT plan, MIN(end_date) AS min_end_date, COUNT(DISTINCT monthly_fee) AS unique_statuss
FROM subscriptions
GROUP BY 1
ORDER BY unique_statuss DESC, plan
LIMIT 5
