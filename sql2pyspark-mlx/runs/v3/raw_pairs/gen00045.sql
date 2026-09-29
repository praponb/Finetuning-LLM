SELECT
    status,
    COUNT(*) row_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) method_values,
    ROUND(AVG(amount), 2) avg_amount
FROM payments
GROUP BY status
ORDER BY status
LIMIT 5;
