SELECT id, plan, start_date, status FROM (SELECT *, RANK() OVER (PARTITION BY plan ORDER BY start_date DESC) AS rnk FROM subscriptions) AS ranked WHERE rnk <= 5 ORDER BY id;
