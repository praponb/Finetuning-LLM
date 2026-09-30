SELECT id, start_date, plan, monthly_fee, ROW_NUMBER() OVER (PARTITION BY YEAR(start_date), plan ORDER BY monthly_fee, id) AS monthly_fee_row FROM subscriptions ORDER BY monthly_fee DESC;
