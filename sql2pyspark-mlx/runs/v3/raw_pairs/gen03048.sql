SELECT DATE_TRUNC('MONTH', order_date) AS order_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values FROM orders GROUP BY 1 ORDER BY order_month;
