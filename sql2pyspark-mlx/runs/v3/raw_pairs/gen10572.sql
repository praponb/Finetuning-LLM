SELECT status, COUNT(monthly_fee) AS monthly_fee_count FROM subscriptions WHERE customer_id = 2 AND customer_id IN (19, 30) AND customer_id IS NULL GROUP BY status HAVING COUNT(monthly_fee) >= 2;
