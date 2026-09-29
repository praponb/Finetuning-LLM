SELECT QUARTER(end_date) AS end_quarter, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values FROM subscriptions GROUP BY QUARTER(end_date) ORDER BY end_quarter DESC, status_values
