SELECT order_id, CASE WHEN status IN ('FAILED', 'REFUNDED') THEN 'core' ELSE 'other' END AS status_group, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM payments
GROUP BY order_id, CASE WHEN status IN ('FAILED', 'REFUNDED') THEN 'core' ELSE 'other' END
