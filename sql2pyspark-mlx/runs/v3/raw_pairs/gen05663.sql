SELECT method, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM payments
WHERE (HOUR(paid_at) BETWEEN 6 AND 9 AND amount BETWEEN 650 AND 1250) OR order_id IS NOT NULL
GROUP BY method;
