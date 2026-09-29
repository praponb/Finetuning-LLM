WITH top_subscriptions AS (SELECT *, RANK() OVER (PARTITION BY status ORDER BY monthly_fee DESC) rn FROM subscriptions) SELECT * FROM top_subscriptions WHERE rn <= 3 ORDER BY status DESC;
