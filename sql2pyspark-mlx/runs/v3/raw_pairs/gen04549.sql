SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) plan_values, COUNT(monthly_fee) monthly_fee_count FROM subscriptions ORDER BY monthly_fee_count ASC;
