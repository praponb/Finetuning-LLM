SELECT status, DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM accounts
GROUP BY status, DATE_FORMAT(opened_date, 'yyyy-MM');
