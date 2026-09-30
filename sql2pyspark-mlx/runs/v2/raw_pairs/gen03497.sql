WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC, id) rnk FROM subscriptions) SELECT id, status, end_date FROM ranked WHERE rnk = 1 ORDER BY status
