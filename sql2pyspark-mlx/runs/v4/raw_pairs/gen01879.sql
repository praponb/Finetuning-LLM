SELECT customer_id, COUNT(DISTINCT monthly_fee) AS unique_statuss, MIN(monthly_fee) AS min_monthly_fee
FROM subscriptions
WHERE end_date BETWEEN '2024-07-20' AND '2025-01-01'
GROUP BY 1
ORDER BY customer_id;
