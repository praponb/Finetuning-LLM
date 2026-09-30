SELECT status, customer_id, ROUND(monthly_fee * 0.9, 2) AS monthly_fee_scaled
FROM subscriptions
WHERE YEAR(end_date) >= 2025 AND start_date < '2023-07-01';
