SELECT * FROM (SELECT status, COUNT(*) num_payments, SORT_ARRAY(COLLECT_SET(status)) status_list FROM payments WHERE order_id = 14 GROUP BY status) x WHERE num_payments < 3 ORDER BY num_payments DESC
