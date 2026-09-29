SELECT id, plan, start_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) AS pos FROM subscriptions) AS ranked WHERE pos <= 2 ORDER BY start_date;
