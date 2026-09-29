SELECT order_id, SORT_ARRAY(COLLECT_SET(carrier)) carrier_list
FROM shipments
WHERE status IN ('RETURNED', 'DELIVERED')
GROUP BY order_id;
