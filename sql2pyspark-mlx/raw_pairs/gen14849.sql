WITH Numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY monthly_fee DESC) AS rn FROM subscriptions) SELECT id, status, monthly_fee, start_date FROM Numbered WHERE rn <= 3;
