SELECT id, plan, end_date, RANK() OVER (PARTITION BY plan ORDER BY end_date) AS rnk FROM subscriptions WHERE start_date BETWEEN '2024-12-01' AND '2025-08-01' ORDER BY id DESC
