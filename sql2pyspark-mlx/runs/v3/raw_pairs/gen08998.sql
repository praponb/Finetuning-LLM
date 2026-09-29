SELECT
    MONTH(paid_at) AS paid_month,
    method,
    COUNT(CASE WHEN method = 'paypal' THEN 1 END) AS paypal_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values
FROM payments
GROUP BY MONTH(paid_at), method
ORDER BY method_values DESC, paid_month;
