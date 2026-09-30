SELECT account_type, MONTH(opened_date) AS opened_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM accounts
GROUP BY account_type, MONTH(opened_date);
