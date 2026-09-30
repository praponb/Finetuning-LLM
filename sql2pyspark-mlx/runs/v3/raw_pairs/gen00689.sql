SELECT MONTH(paid_at) AS paid_month, SORT_ARRAY(COLLECT_SET(method)) AS method_list FROM payments GROUP BY MONTH(paid_at)
