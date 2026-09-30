WITH base AS (
    SELECT *
    FROM payments
    WHERE amount >= 1460 AND (amount > 1420 OR status = 'REFUNDED')
),
summary AS (
    SELECT status, COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count, SORT_ARRAY(COLLECT_SET(method)) AS method_list
    FROM base
    GROUP BY status
)
SELECT status, failed_count, method_list
FROM summary
WHERE failed_count >= 9
