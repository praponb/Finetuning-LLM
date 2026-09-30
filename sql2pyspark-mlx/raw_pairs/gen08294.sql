SELECT
    QUARTER(paid_at) AS paid_quarter,
    COUNT(DISTINCT amount) AS unique_methods,
    ROUND((100 * SUM(CASE WHEN method = 'paypal' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_paypal,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM payments
WHERE method IN ('promptpay', 'paypal') OR status = 'SUCCESS'
GROUP BY QUARTER(paid_at)
ORDER BY unique_methods, paid_quarter
LIMIT 50
