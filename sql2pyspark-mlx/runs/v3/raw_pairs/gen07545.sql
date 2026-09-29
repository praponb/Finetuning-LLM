WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) pos FROM subscriptions) SELECT id, status, monthly_fee, end_date FROM numbered WHERE pos <= 1
