SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list, MIN(start_date) AS first_start_date FROM subscriptions GROUP BY status;
