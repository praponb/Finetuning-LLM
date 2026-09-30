SELECT method, SUM(CASE WHEN status = 'REFUNDED' THEN amount ELSE 0 END) refunded_amount, SORT_ARRAY(COLLECT_SET(method)) method_list FROM payments GROUP BY method ORDER BY refunded_amount DESC;
