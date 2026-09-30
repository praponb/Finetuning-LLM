SELECT id, customer_id, monthly_fee, plan FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS rn FROM subscriptions) AS r WHERE rn <= 3
