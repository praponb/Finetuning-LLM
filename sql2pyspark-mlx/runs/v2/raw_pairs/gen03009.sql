SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values
FROM subscriptions
GROUP BY status
HAVING COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) >= 1
ORDER BY status_values ASC, status ASC
LIMIT 10
