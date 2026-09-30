SELECT * FROM (SELECT id, plan, end_date, status, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) AS rn FROM subscriptions) AS r WHERE rn <= 5 ORDER BY id DESC
