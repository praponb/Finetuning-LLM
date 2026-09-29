WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY status ORDER BY end_date DESC) AS rnk FROM subscriptions) SELECT id, status, end_date, plan FROM ranked WHERE rnk <= 2;
