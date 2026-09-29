SELECT YEAR(opened_date) AS opened_year, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values FROM accounts GROUP BY YEAR(opened_date)
