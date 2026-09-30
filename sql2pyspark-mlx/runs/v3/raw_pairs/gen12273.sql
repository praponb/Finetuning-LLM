WITH top_subscriptions AS (SELECT *, RANK() OVER (PARTITION BY status ORDER BY end_date) rn FROM subscriptions) SELECT * FROM top_subscriptions WHERE rn <= 3
