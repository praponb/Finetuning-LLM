SELECT o.id, o.amount, o.order_date, t.method_list FROM orders o LEFT JOIN (SELECT order_id, SORT_ARRAY(COLLECT_SET(method)) method_list FROM payments GROUP BY order_id) t ON o.id = t.order_id
