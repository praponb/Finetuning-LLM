WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE warehouse_id <= 3
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(starts_session - AVG(starts_session) OVER (PARTITION BY warehouse_id), 2) AS starts_session_vs_warehouse_id_avg,
        SUM(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_sum_6_prior,
        MAX(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_so_far,
        ROUND(session_no / NULLIF(SUM(session_no) OVER (PARTITION BY warehouse_id), 0), 4) AS session_no_share_of_warehouse_id,
        SIZE(COLLECT_SET(sku_id) OVER w_warehouse_id_2) AS distinct_sku_id_in_warehouse_id,
        ROUND(AVG(starts_session) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS starts_session_avg_6_prior
    FROM sessionized
    WINDOW w_warehouse_id_2 AS (PARTITION BY warehouse_id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        qty,
        moved_at,
        moved_at_date,
        prev_moved_at,
        starts_session,
        session_no,
        starts_session_vs_warehouse_id_avg,
        unit_price_sum_6_prior,
        max_unit_price_so_far,
        session_no_share_of_warehouse_id,
        distinct_sku_id_in_warehouse_id,
        starts_session_avg_6_prior,
        SIZE(COLLECT_SET(warehouse_id) OVER w_sku_id) AS distinct_warehouse_id_in_sku_id,
        LEAD(unit_price, 1, 0) OVER (PARTITION BY starts_session ORDER BY prev_moved_at DESC, id) AS next_unit_price,
        SUM(max_unit_price_so_far) OVER (ORDER BY qty ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS max_unit_price_so_far_sum_6_prior
    FROM with_windows
    WINDOW w_sku_id AS (PARTITION BY sku_id)
)
SELECT
    id,
    warehouse_id,
    qty,
    moved_at_date,
    prev_moved_at,
    starts_session,
    session_no,
    starts_session_vs_warehouse_id_avg,
    session_no_share_of_warehouse_id,
    starts_session_avg_6_prior,
    distinct_warehouse_id_in_sku_id,
    max_unit_price_so_far_sum_6_prior,
    ROUND(max_unit_price_so_far * 1.1, 2) AS max_unit_price_so_far_adj
FROM ranked
WHERE warehouse_id IS NOT NULL
ORDER BY starts_session_vs_warehouse_id_avg, session_no, id
LIMIT 20
