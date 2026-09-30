SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list FROM subscriptions WHERE customer_id IS NOT NULL GROUP BY 1;
