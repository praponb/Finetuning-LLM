SELECT id, plan, end_date FROM (SELECT id, plan, end_date, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) AS pos FROM subscriptions) AS ranked WHERE pos <= 2 ORDER BY end_date;
