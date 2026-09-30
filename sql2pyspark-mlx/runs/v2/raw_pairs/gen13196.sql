SELECT customer_id, COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) AS active_count, MAX(monthly_fee) AS max_monthly_fee
FROM subscriptions
WHERE customer_id IN (30, 36, 37) AND status != 'CANCELLED' AND end_date > '2024-08-01'
GROUP BY customer_id
