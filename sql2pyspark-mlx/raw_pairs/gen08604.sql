WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        LEAD(cost) OVER w_running AS next_cost,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM shipments
    WHERE cost BETWEEN 73 AND 90
    WINDOW w_running AS (PARTITION BY status ORDER BY shipped_date, id)
)
SELECT
    id,
    carrier,
    delivered_date,
    cost,
    next_cost
FROM with_windows
ORDER BY cost;
