WITH top_subscriptions AS (SELECT id, status, end_date, ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC, id) AS rn FROM subscriptions) SELECT * FROM top_subscriptions WHERE rn <= 3
