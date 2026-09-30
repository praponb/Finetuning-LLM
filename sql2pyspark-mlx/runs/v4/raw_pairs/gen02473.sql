SELECT end_date, LAST_DAY(start_date) AS start_date_month_end, CASE WHEN plan IN ('premium', 'enterprise') THEN 'Core' ELSE 'Other' END AS plan_group FROM subscriptions
