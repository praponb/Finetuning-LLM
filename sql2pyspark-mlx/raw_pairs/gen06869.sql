WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) rnk FROM subscriptions) SELECT * FROM ranked WHERE rnk <= 3
