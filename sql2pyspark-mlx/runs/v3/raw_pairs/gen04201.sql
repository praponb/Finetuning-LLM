SELECT status, customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values
FROM orders
GROUP BY 1, 2
ORDER BY customer_id;
