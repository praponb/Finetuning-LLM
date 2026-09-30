SELECT monthly_fee, INITCAP(TRIM(plan)) AS plan_title, UPPER(plan) AS plan_upper FROM subscriptions WHERE status IN ('EXPIRED', 'ACTIVE', 'CANCELLED') AND status = 'EXPIRED'
