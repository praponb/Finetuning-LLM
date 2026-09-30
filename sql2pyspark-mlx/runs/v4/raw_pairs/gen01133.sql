SELECT id, status, monthly_fee FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS rn FROM subscriptions WHERE monthly_fee IS NOT NULL) AS t WHERE rn <= 5
