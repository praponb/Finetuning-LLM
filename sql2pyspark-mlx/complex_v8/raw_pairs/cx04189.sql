WITH filtered AS (
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
    WHERE moved_at >= TIMESTAMP '2025-02-06 08:00:00'
      AND sku_id IS NOT NULL
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        operator,
        unit_price,
        SUM(unit_price) OVER (PARTITION BY operator, move_type ORDER BY unit_price DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS unit_price_sum_7,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER w_warehouse_id_move_type_by_un)) / 60, 1) AS minutes_since_prev_moved_at,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type,
        SUM(unit_price) OVER (PARTITION BY move_type ORDER BY qty ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS unit_price_sum_3,
        unit_price - LAG(unit_price) OVER w_operator_move_type_by_unit_p_2 AS unit_price_change,
        NTILE(4) OVER (PARTITION BY warehouse_id, move_type ORDER BY unit_price, id) AS unit_price_quartile,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY move_type)) AS distinct_warehouse_id_in_move_type,
        MAX(qty) OVER w_move_type AS move_type_max_qty,
        ROW_NUMBER() OVER w_move_type_by_qty AS rn,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_move_type, 0), 4) AS unit_price_share_of_move_type,
        SIZE(COLLECT_SET(operator) OVER w_move_type) AS distinct_operator_in_move_type,
        NTILE(4) OVER (PARTITION BY move_type ORDER BY qty ASC, id) AS qty_quartile,
        RANK() OVER (PARTITION BY warehouse_id, move_type ORDER BY unit_price) AS unit_price_rank
    FROM filtered
    WINDOW w_warehouse_id_move_type_by_un AS (PARTITION BY warehouse_id, move_type ORDER BY unit_price, id), w_move_type AS (PARTITION BY move_type), w_operator_move_type_by_unit_p_2 AS (PARTITION BY operator, move_type ORDER BY unit_price DESC, id), w_move_type_by_qty AS (PARTITION BY move_type ORDER BY qty ASC, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        operator,
        sku_id,
        distinct_operator_in_move_type,
        COUNT(*) AS cnt,
        ROUND(SUM(unit_price_rank) / NULLIF(SUM(unit_price_change), 0), 3) AS unit_price_rank_to_unit_price_change_ratio
    FROM with_windows
    GROUP BY operator, sku_id, distinct_operator_in_move_type
    HAVING COUNT(*) >= 2
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(operator) OVER w_distinct_operator_in_move_ty) AS distinct_operator_in_distinct_operator_in_move_t,
        ROW_NUMBER() OVER (ORDER BY cnt, operator, sku_id, distinct_operator_in_move_type) AS seq_no,
        ROUND(cnt / NULLIF(SUM(cnt) OVER w_distinct_operator_in_move_ty, 0), 4) AS cnt_share_of_distinct_operator_in_move_type
    FROM grouped
    WINDOW w_distinct_operator_in_move_ty AS (PARTITION BY distinct_operator_in_move_type)
)
SELECT
    operator,
    sku_id,
    distinct_operator_in_move_type,
    cnt,
    unit_price_rank_to_unit_price_change_ratio,
    distinct_operator_in_distinct_operator_in_move_t,
    seq_no,
    cnt_share_of_distinct_operator_in_move_type,
    LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
FROM windowed
ORDER BY sku_id DESC NULLS FIRST, seq_no DESC
