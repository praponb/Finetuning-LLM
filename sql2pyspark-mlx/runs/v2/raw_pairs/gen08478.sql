SELECT status, COUNT(shipping_fee) shipping_fee_count, SORT_ARRAY(COLLECT_SET(channel)) channel_list FROM orders GROUP BY status HAVING COUNT(shipping_fee) < 5 ORDER BY status;
