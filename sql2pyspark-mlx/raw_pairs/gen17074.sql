SELECT status, COUNT(CASE WHEN method = 'promptpay' THEN 1 END) AS promptpay_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values
FROM payments
WHERE HOUR(paid_at) BETWEEN 11 AND 15 AND amount < 880
GROUP BY status
ORDER BY status DESC
