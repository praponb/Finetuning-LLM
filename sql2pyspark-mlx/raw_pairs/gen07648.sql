SELECT status, plan, COUNT(monthly_fee) AS monthly_fee_count FROM subscriptions WHERE plan NOT IN ('basic', 'premium') AND start_date BETWEEN '2024-02-01' AND '2025-01-01' GROUP BY status, plan
