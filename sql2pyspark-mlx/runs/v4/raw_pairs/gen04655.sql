SELECT YEAR(order_date) AS order_year, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values FROM orders WHERE MONTH(order_date) IN (1, 3, 11) GROUP BY order_year;
