SELECT
    status,
    method,
    SUM(amount) total_amount,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values
FROM payments
GROUP BY status, method
HAVING SUM(amount) > 7400;
