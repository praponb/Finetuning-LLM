SELECT status, plan, MAX(monthly_fee) AS max_monthly_fee FROM subscriptions WHERE monthly_fee BETWEEN 100 AND 160 OR status = 'EXPIRED' GROUP BY status, plan;
