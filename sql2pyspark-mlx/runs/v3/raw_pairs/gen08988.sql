SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, SUM(balance) AS total_balance, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM accounts
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM');
