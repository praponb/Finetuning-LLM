SELECT id, plan, end_date, RANK() OVER (PARTITION BY plan ORDER BY end_date) AS rnk FROM subscriptions WHERE start_date > '2023-07-01' ORDER BY plan, id DESC;
