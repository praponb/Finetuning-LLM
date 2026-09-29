SELECT method, SORT_ARRAY(COLLECT_SET(method)) AS method_list FROM payments WHERE NOT (status <> 'REFUNDED' AND status = 'REFUNDED') GROUP BY 1 ORDER BY method;
