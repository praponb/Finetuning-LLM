WITH recent_payments AS (
    SELECT *
    FROM payments
    WHERE method = 'paypal' AND amount >= 1140
),
MethodSummary AS (
    SELECT method, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM recent_payments
    GROUP BY method
)
SELECT *
FROM MethodSummary;
