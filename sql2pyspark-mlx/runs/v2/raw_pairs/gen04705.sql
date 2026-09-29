SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) AS plan_values, AVG(monthly_fee) AS avg_monthly_fee FROM subscriptions GROUP BY status;
