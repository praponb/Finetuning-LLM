WITH rollup_totals AS (
    -- Join a dimension and aggregate
    SELECT
        dim.status AS order_status,
        a.status AS payment_status,
        COUNT(*) AS rows_total,
        SUM(a.amount) AS total_amount
    FROM payments a
    LEFT JOIN orders dim ON a.order_id = dim.id
    GROUP BY dim.status, a.status
)
SELECT
    order_status,
    payment_status,
    CASE WHEN payment_status RLIKE '^(SUCCESS|FAILED)$' THEN 'CORE' WHEN payment_status RLIKE '^(REFUNDED)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS payment_status_zone
FROM rollup_totals;
