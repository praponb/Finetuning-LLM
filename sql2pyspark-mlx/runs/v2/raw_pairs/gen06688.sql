SELECT
    YEAR(paid_at) AS paid_year,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
    MAX(amount) - MIN(amount) AS amount_range
FROM payments
WHERE method = 'promptpay'
GROUP BY YEAR(paid_at)
ORDER BY paid_year
