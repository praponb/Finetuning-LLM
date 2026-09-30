SELECT DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month, SORT_ARRAY(COLLECT_SET(method)) AS method_list, COUNT(CASE WHEN method = 'cash' THEN 1 END) AS cash_count FROM payments GROUP BY paid_month;
