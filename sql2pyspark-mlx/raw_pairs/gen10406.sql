SELECT status, SORT_ARRAY(COLLECT_SET(method)) AS method_list
FROM payments
WHERE HOUR(paid_at) BETWEEN 11 AND 15 AND order_id IS NULL
GROUP BY status;
