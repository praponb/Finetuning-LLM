SELECT SORT_ARRAY(COLLECT_SET(plan)) AS plan_list, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range, COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) AS active_count FROM subscriptions
