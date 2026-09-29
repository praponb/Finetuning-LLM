SELECT * FROM (SELECT id, status, start_date, plan, ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id) AS rnk FROM subscriptions) AS t WHERE rnk = 1 ORDER BY status DESC, plan;
