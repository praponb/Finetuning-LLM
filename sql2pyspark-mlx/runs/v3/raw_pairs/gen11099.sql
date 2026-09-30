SELECT status, SORT_ARRAY(COLLECT_SET(status)) status_list FROM subscriptions GROUP BY status;
