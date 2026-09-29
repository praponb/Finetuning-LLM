WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day
    FROM stock_moves
    WHERE (unit_price < 367.7 OR qty <= 256)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        COUNT(*) OVER w_moved_at_day_to_date_sku_id AS rows_in_moved_at_day_to_date_sku_id,
        RANK() OVER (ORDER BY moved_at DESC) AS moved_at_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at DESC), 3) AS moved_at_percent_rank
    FROM prepared_moves
    WINDOW w_moved_at_day_to_date_sku_id AS (PARTITION BY TO_DATE(moved_at_day), sku_id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        moved_at_day,
        second_unit_price,
        moved_at_rank,
        moved_at_percent_rank,
        LAST_VALUE(moved_at_day) IGNORE NULLS OVER (PARTITION BY sku_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at_day,
        SIZE(COLLECT_SET(operator) OVER w_moved_at_rank_move_type) AS distinct_operator_in_moved_at_rank_move_type,
        SIZE(COLLECT_SET(warehouse_id) OVER w_moved_at_rank_move_type) AS distinct_warehouse_id_in_moved_at_rank_move_type,
        COUNT(*) OVER (PARTITION BY moved_at_rank, move_type) AS rows_in_moved_at_rank_move_type
    FROM metrics
    WINDOW w_moved_at_rank_move_type AS (PARTITION BY moved_at_rank, move_type)
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', last_known_moved_at_day) AS last_known_moved_at_day_month,
        YEAR(moved_at) AS moved_at_year,
        MAX(qty) AS max_qty,
        SUM(qty) AS total_qty,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        SUM(CASE WHEN move_type IS NOT NULL THEN 1 ELSE 0 END) AS n_move_type_is_not_null,
        ROUND(SUM(moved_at_percent_rank) / NULLIF(SUM(second_unit_price), 0), 3) AS moved_at_percent_rank_to_second_unit_price_ratio
    FROM with_windows
    GROUP BY DATE_TRUNC('MONTH', last_known_moved_at_day), YEAR(moved_at)
),
exploded AS (
    -- One row per operator_list_item
    SELECT
        s.last_known_moved_at_day_month,
        s.moved_at_year,
        s.max_qty,
        s.total_qty,
        s.n_move_type_is_not_null,
        s.moved_at_percent_rank_to_second_unit_price_ratio,
        t.operator_list_item
    FROM grouped s
    LATERAL VIEW EXPLODE(s.operator_list) t AS operator_list_item
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        last_known_moved_at_day_month,
        moved_at_year,
        max_qty,
        n_move_type_is_not_null,
        moved_at_percent_rank_to_second_unit_price_ratio,
        operator_list_item,
        ROUND(total_qty / NULLIF(SUM(total_qty) OVER w_n_move_type_is_not_null_move, 0), 4) AS total_qty_share_of_n_move_type_is_not_null_moved,
        ROUND(AVG(moved_at_year) OVER (PARTITION BY n_move_type_is_not_null, moved_at_year), 2) AS n_move_type_is_not_null_moved_at_year_avg_moved_,
        NTILE(5) OVER (PARTITION BY n_move_type_is_not_null ORDER BY total_qty, last_known_moved_at_day_month, moved_at_year, operator_list_item) AS total_qty_quintile,
        ROUND(AVG(moved_at_year) OVER (PARTITION BY n_move_type_is_not_null ORDER BY total_qty, last_known_moved_at_day_month, moved_at_year, operator_list_item ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS moved_at_year_avg_2_prior
    FROM exploded
    WINDOW w_n_move_type_is_not_null_move AS (PARTITION BY n_move_type_is_not_null, moved_at_year)
),
enriched AS (
    -- Label and score rows
    SELECT
        last_known_moved_at_day_month,
        moved_at_year,
        max_qty,
        operator_list_item,
        total_qty_share_of_n_move_type_is_not_null_moved,
        n_move_type_is_not_null_moved_at_year_avg_moved_,
        total_qty_quintile,
        CASE WHEN n_move_type_is_not_null >= 7 THEN 'HIGH' WHEN n_move_type_is_not_null >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS n_move_type_is_not_null_tier
    FROM with_windows_2
)
-- Final projection
SELECT
    last_known_moved_at_day_month,
    moved_at_year,
    operator_list_item,
    total_qty_share_of_n_move_type_is_not_null_moved,
    n_move_type_is_not_null_moved_at_year_avg_moved_,
    total_qty_quintile,
    n_move_type_is_not_null_tier,
    ROUND(total_qty_quintile / NULLIF(moved_at_year, 0), 3) AS total_qty_quintile_per_moved_at_year
FROM enriched
