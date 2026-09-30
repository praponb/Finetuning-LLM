SELECT id, plan, end_date, status, RANK() OVER (PARTITION BY plan ORDER BY end_date) AS rnk, DENSE_RANK() OVER (PARTITION BY status ORDER BY end_date DESC) AS dense_rnk FROM subscriptions ORDER BY id
