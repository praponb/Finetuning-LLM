SELECT id, plan, status, end_date, RANK() OVER (PARTITION BY plan, status ORDER BY end_date DESC) AS end_date_rank FROM subscriptions;
