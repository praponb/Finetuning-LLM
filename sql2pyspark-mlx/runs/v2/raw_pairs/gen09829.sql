WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) rn FROM subscriptions) SELECT * FROM ranked WHERE rn = 1 ORDER BY status DESC
