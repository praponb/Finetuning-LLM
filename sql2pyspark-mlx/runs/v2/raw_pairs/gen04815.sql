SELECT
    method,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    MIN(paid_at) AS first_paid_at,
    MAX(amount) AS max_amount
FROM payments
WHERE method <> 'cash' AND amount > 820
GROUP BY method
ORDER BY method ASC
