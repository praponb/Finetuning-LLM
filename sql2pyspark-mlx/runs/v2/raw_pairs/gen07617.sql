SELECT id, monthly_fee, plan, DATE_SUB(end_date, 90) AS end_date_minus_90d FROM subscriptions WHERE status = 'ACTIVE' AND YEAR(start_date) >= 2023;
