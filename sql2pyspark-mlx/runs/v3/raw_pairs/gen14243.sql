SELECT
    status,
    method,
    COUNT(amount) AS amount_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values,
    ROUND(AVG(amount), 2) AS avg_amount
FROM payments
GROUP BY status, method;
