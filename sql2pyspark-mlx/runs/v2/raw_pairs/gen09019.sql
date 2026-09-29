SELECT COUNT(DISTINCT monthly_fee) AS unique_statuss
FROM subscriptions
WHERE customer_id IS NULL AND end_date BETWEEN '2023-08-09' AND '2025-10-01'
ORDER BY 1 DESC;
