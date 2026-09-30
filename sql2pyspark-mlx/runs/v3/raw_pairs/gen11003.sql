SELECT status, store_id, SUM(amount) AS total_amount, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list FROM orders GROUP BY status, store_id ORDER BY store_id DESC;
