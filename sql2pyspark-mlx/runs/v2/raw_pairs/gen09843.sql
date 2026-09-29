SELECT * FROM (SELECT id, status, monthly_fee, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS pos FROM subscriptions) AS ranked WHERE pos <= 1;
