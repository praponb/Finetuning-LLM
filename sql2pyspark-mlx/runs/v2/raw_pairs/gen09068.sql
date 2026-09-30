SELECT id, status FROM subscriptions WHERE start_date BETWEEN '2024-11-01' AND '2025-08-01' UNION ALL SELECT id, status FROM subscriptions WHERE monthly_fee BETWEEN 170 AND 200
