SELECT status, DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month, SORT_ARRAY(COLLECT_SET(status)) AS status_list FROM payments GROUP BY status, DATE_FORMAT(paid_at, 'yyyy-MM') ORDER BY 2 DESC
