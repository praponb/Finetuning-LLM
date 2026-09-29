SELECT status, ROUND(AVG(discount), 2) AS avg_discount, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list, MIN(order_date) AS min_order_date FROM orders GROUP BY status
