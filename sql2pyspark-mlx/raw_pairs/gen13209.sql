WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        LAG(cost) OVER w_recent AS prev_cost,
        LEAD(cost) OVER w_recent AS next_cost,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM shipments
    WINDOW w_recent AS (PARTITION BY status ORDER BY shipped_date, id)
)
SELECT
    id,
    delivered_date,
    status,
    prev_cost,
    next_cost,
    CASE WHEN status RLIKE '^(RETURNED|DELIVERED)$' THEN 'GROUP_A' WHEN status RLIKE '^(IN_TRANSIT|LOST)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS status_zone
FROM with_windows
ORDER BY status_zone DESC, id DESC
