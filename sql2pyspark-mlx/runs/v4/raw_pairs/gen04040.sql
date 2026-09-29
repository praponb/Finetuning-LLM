WITH with_gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
sessions_payments AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    id,
    order_id,
    method,
    status,
    session_id,
    CASE WHEN status RLIKE '^(SUCCESS|FAILED)$' THEN 'OTHER_ZONE' WHEN status RLIKE '^(REFUNDED)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS status_zone
FROM sessions_payments
ORDER BY session_id DESC NULLS LAST;
