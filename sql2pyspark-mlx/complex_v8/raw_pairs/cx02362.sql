WITH valid AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
    WHERE operator = 'op_bee'
      AND warehouse_id <= 5
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        operator_len,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY sku_id, TO_DATE(moved_at)), 0), 4) AS qty_share_of_sku_id_moved_at_to_date,
        LAG(moved_at, 3) OVER (PARTITION BY warehouse_id, move_type ORDER BY operator_len, id) AS prev3_moved_at,
        MAX(operator_len) OVER (PARTITION BY sku_id, TO_DATE(moved_at)) AS sku_id_moved_at_to_date_max_operator_len,
        ROUND(AVG(qty) OVER w_warehouse_id, 2) AS warehouse_id_avg_qty,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), operator_len), 0), 4) AS qty_share_of_moved_at_month_operator_len,
        MIN(operator_len) OVER (PARTITION BY sku_id, TO_DATE(moved_at)) AS sku_id_moved_at_to_date_min_operator_len,
        RANK() OVER (PARTITION BY warehouse_id ORDER BY unit_price DESC NULLS FIRST) AS unit_price_rank,
        ROUND(operator_len - AVG(operator_len) OVER (PARTITION BY warehouse_id), 2) AS operator_len_vs_warehouse_id_avg,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_3_prior,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_sku_id_moved_at_to_date, 0), 4) AS unit_price_share_of_sku_id_moved_at_to_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY unit_price DESC NULLS FIRST), 3) AS unit_price_percent_rank
    FROM valid
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id), w_sku_id_moved_at_to_date AS (PARTITION BY sku_id, TO_DATE(moved_at))
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE unit_price_rank <= 5
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(unit_price_rank - AVG(unit_price_rank) OVER (PARTITION BY unit_price_rank, TO_DATE(prev3_moved_at)), 2) AS unit_price_rank_vs_unit_price_rank_prev3_moved_a,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY unit_price_rank), 0), 4) AS unit_price_share_of_unit_price_rank,
        COUNT(*) OVER (PARTITION BY unit_price_rank, TO_DATE(prev3_moved_at)) AS rows_in_unit_price_rank_prev3_moved_at_to_date,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY unit_price_rank, TO_DATE(prev3_moved_at))) AS distinct_move_type_in_unit_price_rank_prev3_move
    FROM top_rows
)
SELECT
    id,
    sku_id,
    qty,
    moved_at,
    operator_len,
    qty_share_of_sku_id_moved_at_to_date,
    sku_id_moved_at_to_date_max_operator_len,
    warehouse_id_avg_qty,
    qty_share_of_moved_at_month_operator_len,
    unit_price_rank,
    unit_price_share_of_sku_id_moved_at_to_date,
    unit_price_percent_rank,
    unit_price_rank_vs_unit_price_rank_prev3_moved_a,
    unit_price_share_of_unit_price_rank,
    rows_in_unit_price_rank_prev3_moved_at_to_date,
    distinct_move_type_in_unit_price_rank_prev3_move,
    moved_at + INTERVAL 15 MINUTES AS moved_at_plus_15m
FROM ranked
ORDER BY id DESC NULLS FIRST, sku_id_moved_at_to_date_max_operator_len
LIMIT 10
