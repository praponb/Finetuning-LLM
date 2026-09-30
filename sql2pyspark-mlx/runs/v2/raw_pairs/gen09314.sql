SELECT id, plan, end_date, RANK() OVER (PARTITION BY plan ORDER BY end_date DESC) AS end_date_rank FROM subscriptions;
