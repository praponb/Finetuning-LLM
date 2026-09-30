SELECT MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list, SUM(shipping_fee) AS total_shipping_fee
FROM orders
WHERE discount < 0.35;
