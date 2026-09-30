SELECT plan, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values FROM subscriptions WHERE monthly_fee >= 130 GROUP BY plan;
