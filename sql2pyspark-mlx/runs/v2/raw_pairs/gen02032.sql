SELECT status, SORT_ARRAY(COLLECT_SET(status)) status_list
FROM orders
WHERE channel IN ('web', 'mobile')
GROUP BY status
ORDER BY status DESC;
