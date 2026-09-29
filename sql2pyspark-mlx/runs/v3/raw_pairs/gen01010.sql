WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id) AS rn FROM subscriptions) SELECT * FROM ranked WHERE rn <= 3;
