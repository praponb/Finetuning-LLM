SELECT id, status, start_date, end_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date, id) rnk FROM subscriptions) t WHERE rnk <= 2 ORDER BY 1;
