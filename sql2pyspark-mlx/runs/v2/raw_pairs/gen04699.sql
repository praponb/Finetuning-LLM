WITH src AS (
    SELECT *
    FROM payments
    WHERE amount >= 1420 AND status = 'REFUNDED' AND order_id IS NOT NULL
),
summary AS (
    SELECT method, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values, ROUND(AVG(amount), 2) AS avg_amount
    FROM src
    GROUP BY method
)
SELECT *
FROM summary
WHERE avg_amount < 32
ORDER BY method DESC, avg_amount DESC;
