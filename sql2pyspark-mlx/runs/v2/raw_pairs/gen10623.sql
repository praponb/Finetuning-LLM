SELECT id, plan, ADD_MONTHS(start_date, 3) AS start_date_plus_months FROM subscriptions WHERE status IN ('ACTIVE', 'PAUSED')
