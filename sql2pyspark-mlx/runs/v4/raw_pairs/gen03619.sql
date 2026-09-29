WITH src AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at
    FROM payments
    WHERE status = 'SUCCESS' AND (status <> 'SUCCESS' OR method IN ('paypal', 'card', 'promptpay'))
),
status_summary AS (
    SELECT status, MAX(paid_at) AS latest_paid_at
    FROM src
    GROUP BY status
)
SELECT status, latest_paid_at
FROM status_summary
