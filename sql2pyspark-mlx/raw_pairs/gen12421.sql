SELECT order_id, SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list FROM shipments WHERE cost BETWEEN 31 AND 37 GROUP BY order_id ORDER BY order_id DESC;
