SELECT status, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list, COUNT(DISTINCT discount) AS unique_customers FROM orders GROUP BY status HAVING COUNT(DISTINCT discount) < 4 ORDER BY status
