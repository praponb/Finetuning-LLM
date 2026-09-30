WITH valid_payments AS (
    SELECT *, TRIM(status) AS clean_status
    FROM payments
    WHERE amount > 620
),
summary AS (
    SELECT method, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values
    FROM valid_payments
    GROUP BY method
)
SELECT *
FROM summary
ORDER BY method
LIMIT 50;
