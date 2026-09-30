SELECT plan, SUM(monthly_fee) total_monthly_fee, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values, COUNT(*) num_subscriptions FROM subscriptions GROUP BY plan ORDER BY plan;
