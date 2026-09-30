WITH cleaned_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM stock_moves
    WHERE unit_price >= 236.5
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type, sku_id ORDER BY moved_at ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_2_prior,
        ROUND(CUME_DIST() OVER (ORDER BY qty, id), 3) AS qty_cume_dist,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER w_move_type_sku_id_by_moved_at_2)) / 60, 1) AS minutes_since_prev_moved_at,
        LEAD(qty) OVER w_warehouse_id_move_type_by_mo AS next_qty,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at), 3) AS moved_at_percent_rank
    FROM cleaned_moves
    WINDOW w_move_type_sku_id_by_moved_at_2 AS (PARTITION BY move_type, sku_id ORDER BY moved_at ASC NULLS LAST, id), w_warehouse_id_move_type_by_mo AS (PARTITION BY warehouse_id, move_type ORDER BY moved_at, id)
),
latest AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price_avg_2_prior,
        qty_cume_dist,
        minutes_since_prev_moved_at,
        next_qty,
        moved_at_percent_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY next_qty DESC, id DESC) AS rn
        FROM sequenced
    ) ranked
    WHERE rn = 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        unit_price - LAG(unit_price) OVER (PARTITION BY operator ORDER BY qty_cume_dist ASC NULLS LAST, id) AS unit_price_change,
        LAST_VALUE(minutes_since_prev_moved_at) IGNORE NULLS OVER (PARTITION BY operator ORDER BY qty_cume_dist ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_minutes_since_prev_moved_at
    FROM latest
),
latest_2 AS (
    -- Keep the latest row per move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price_avg_2_prior,
        qty_cume_dist,
        minutes_since_prev_moved_at,
        next_qty,
        moved_at_percent_rank,
        unit_price_change,
        last_known_minutes_since_prev_moved_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY unit_price_change DESC, id DESC) AS recency_rank
        FROM windowed
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT move_type, operator, minutes_since_prev_moved_at
    FROM latest_2
) src
PIVOT (
    AVG(minutes_since_prev_moved_at)
    FOR operator IN ('op_dew', 'op_eve', 'op_ann', 'op_bee')
)
ORDER BY move_type
