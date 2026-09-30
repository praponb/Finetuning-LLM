SELECT id, plan, start_date, ADD_MONTHS(start_date, 1) start_date_plus_months FROM subscriptions WHERE start_date >= '2025-08-01' AND plan = 'premium' AND customer_id = 30;
