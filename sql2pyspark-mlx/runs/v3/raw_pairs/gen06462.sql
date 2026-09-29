SELECT channel, status, MIN(order_date) min_order_date, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values FROM orders WHERE customer_id IS NOT NULL GROUP BY channel, status;
