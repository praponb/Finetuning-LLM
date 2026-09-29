SELECT
    method,
    SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END) AS cash_count,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    MIN(amount) AS min_amount
FROM payments
WHERE amount >= 420 AND paid_at >= '2023-08-01' AND HOUR(paid_at) BETWEEN 7 AND 15
GROUP BY method
HAVING SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END) > 22
ORDER BY method DESC
