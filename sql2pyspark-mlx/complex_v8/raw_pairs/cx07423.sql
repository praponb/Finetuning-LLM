WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator NOT IN ('op_bee', 'op_chan')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(moved_at, 3) OVER w_all_by_moved_at AS prev3_moved_at,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY move_type), 0), 4) AS unit_price_share_of_move_type,
        ROUND(unit_price - AVG(unit_price) OVER w_move_type, 2) AS unit_price_vs_move_type_avg
    FROM recent
    WINDOW w_all_by_moved_at AS (ORDER BY moved_at ASC, id), w_move_type AS (PARTITION BY move_type)
),
latest_per_key AS (
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
        prev3_moved_at,
        unit_price_share_of_move_type,
        unit_price_vs_move_type_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY unit_price DESC, id DESC) AS recency_rank
        FROM metrics
    ) ranked
    WHERE recency_rank = 1
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        st.id AS move_id,
        st.sku_id AS move_sku_id,
        st.move_type AS move_move_type
    FROM latest_per_key s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.sku_id IS NOT NULL
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.move_sku_id) AS move_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.move_sku_id) AS move_count_2,
        (SELECT COUNT(*) FILTER (WHERE st.move_type = 'TRANSFER') FROM stock_moves st WHERE st.sku_id = s.move_sku_id) AS move_move_type_count
    FROM exploded_detail_moves s
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        qty,
        moved_at,
        operator,
        move_id,
        move_sku_id,
        move_move_type,
        move_count,
        move_move_type_count,
        MIN(unit_price) OVER (ORDER BY move_move_type_count DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_so_far,
        LAST_VALUE(move_move_type_count) OVER (PARTITION BY move_type, move_id ORDER BY move_move_type_count, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_move_move_type_count,
        MAX(move_count_2) OVER (PARTITION BY move_move_type_count) AS move_move_type_count_max_move_count_2,
        COUNT(*) OVER (PARTITION BY move_move_type_count) AS rows_in_move_move_type_count,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY move_id ORDER BY move_move_type_count ASC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        RANK() OVER w_all_by_move_move_type_count_2 AS move_move_type_count_rank
    FROM with_counts
    WINDOW w_all_by_move_move_type_count_2 AS (ORDER BY move_move_type_count DESC, id, move_id)
),
per_group AS (
    -- Group-level totals
    SELECT
        last_move_move_type_count,
        move_move_type,
        DATE_TRUNC('MONTH', first_known_moved_at) AS first_known_moved_at_month,
        ROUND(SUM(min_unit_price_so_far), 2) AS total_min_unit_price_so_far,
        MIN(moved_at) AS first_moved_at
    FROM ranked
    GROUP BY last_move_move_type_count, move_move_type, DATE_TRUNC('MONTH', first_known_moved_at)
    HAVING MAX(move_count) > 2
)
SELECT
    last_move_move_type_count,
    move_move_type,
    first_known_moved_at_month,
    total_min_unit_price_so_far,
    first_moved_at,
    COALESCE(move_move_type, 'UNKNOWN') AS move_move_type_or_unknown
FROM per_group
ORDER BY first_moved_at DESC
