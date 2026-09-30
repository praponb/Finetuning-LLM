SELECT COUNT(DISTINCT shipping_fee) AS unique_channels, MAX(order_date) AS latest_order_date, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
FROM orders
ORDER BY unique_channels DESC;
