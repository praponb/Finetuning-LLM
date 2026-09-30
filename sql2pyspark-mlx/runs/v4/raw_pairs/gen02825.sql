WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(cost, 1) OVER (PARTITION BY order_id, carrier ORDER BY delivered_date ASC, id ASC) AS next_cost,
        SUM(cost) OVER w_part AS running_cost,
        LAST_VALUE(carrier) IGNORE NULLS OVER (PARTITION BY carrier ORDER BY delivered_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier
    FROM shipments
    WINDOW w_part AS (PARTITION BY order_id ORDER BY shipped_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        next_cost,
        running_cost,
        last_known_carrier,
        SUM(running_cost) OVER w_running AS cumulative_running_cost,
        RANK() OVER (PARTITION BY carrier ORDER BY next_cost ASC) AS next_cost_rank
    FROM with_windows
    WINDOW w_running AS (PARTITION BY carrier ORDER BY delivered_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC NULLS FIRST, id ASC) AS rn
    FROM scored
    QUALIFY rn = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT e.*, b.shipping_fee
    FROM latest e
    LEFT JOIN orders b
        ON e.order_id = b.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        carrier,
        status,
        cost
    FROM with_dims
) src
PIVOT (
    MAX(cost) AS total,
    COUNT(1) AS n
    FOR status IN ('LOST', 'RETURNED', 'DELIVERED')
)
