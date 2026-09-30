SELECT TO_DATE(paid_at) AS paid_day, SORT_ARRAY(COLLECT_SET(method)) AS method_list FROM payments WHERE paid_at < '2024-01-01' GROUP BY 1;
