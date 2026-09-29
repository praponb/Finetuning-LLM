SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS rn FROM subscriptions) AS ranked WHERE rn = 1;
