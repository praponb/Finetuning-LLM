SELECT
    channel,
    status,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) channel_values,
    MAX(amount) - MIN(amount) amount_range,
    MAX(order_date) latest_order_date
FROM orders
GROUP BY ROLLUP(channel, status);
