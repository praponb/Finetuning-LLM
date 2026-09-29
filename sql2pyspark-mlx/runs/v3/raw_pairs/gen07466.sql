SELECT * FROM (SELECT id, plan, monthly_fee, status, RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC) AS rn FROM subscriptions) AS t WHERE rn <= 3 ORDER BY id DESC
