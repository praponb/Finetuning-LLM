SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM subscriptions
WHERE (plan IN ('standard', 'premium') AND end_date > '2025-10-01') OR end_date >= '2025-07-28'
GROUP BY status
ORDER BY status DESC;
