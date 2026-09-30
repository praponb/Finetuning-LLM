SELECT * FROM (SELECT status, AVG(monthly_fee) AS avg_monthly_fee, MIN(monthly_fee) AS min_monthly_fee FROM subscriptions GROUP BY status) AS x WHERE avg_monthly_fee < 140;
