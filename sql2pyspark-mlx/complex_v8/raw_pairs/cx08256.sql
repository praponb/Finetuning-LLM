WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        UPPER(TRIM(operator)) AS operator_clean,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
    WHERE operator IN ('op_bee', 'op_ann')
),
with_parent_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.operator_clean,
        s.unit_price_per_qty,
        w.wh_code AS warehouse_wh_code
    FROM recent s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.wh_code IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price_per_qty,
        warehouse_wh_code,
        SIZE(COLLECT_SET(sku_id) OVER w_moved_at_month_warehouse_id) AS distinct_sku_id_in_moved_at_month_warehouse_id,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY operator ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        LAST_VALUE(unit_price_per_qty) OVER (PARTITION BY operator ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price_per_qty,
        LAST_VALUE(unit_price_per_qty) IGNORE NULLS OVER (PARTITION BY operator ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price_per_qty,
        SIZE(COLLECT_SET(sku_id) OVER w_warehouse_id_move_type) AS distinct_sku_id_in_warehouse_id_move_type,
        LEAD(unit_price_per_qty) OVER w_operator_by_unit_price_per_q AS next_unit_price_per_qty,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY operator ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        ROUND(unit_price_per_qty - AVG(unit_price_per_qty) OVER (PARTITION BY warehouse_id, move_type), 2) AS unit_price_per_qty_vs_warehouse_id_move_type_avg,
        ROUND(CUME_DIST() OVER (ORDER BY unit_price_per_qty ASC), 3) AS unit_price_per_qty_cume_dist,
        ROUND(unit_price_per_qty / NULLIF(SUM(unit_price_per_qty) OVER (PARTITION BY MONTH(moved_at), warehouse_id), 0), 4) AS unit_price_per_qty_share_of_moved_at_month_wareh,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY MONTH(moved_at), warehouse_id)) AS distinct_move_type_in_moved_at_month_warehouse_i,
        MIN(unit_price_per_qty) OVER w_warehouse_id_move_type AS warehouse_id_move_type_min_unit_price_per_qty,
        ROUND(AVG(unit_price_per_qty) OVER w_warehouse_id_move_type, 2) AS warehouse_id_move_type_avg_unit_price_per_qty
    FROM with_parent_warehouse
    WINDOW w_moved_at_month_warehouse_id AS (PARTITION BY MONTH(moved_at), warehouse_id), w_warehouse_id_move_type AS (PARTITION BY warehouse_id, move_type), w_operator_by_unit_price_per_q AS (PARTITION BY operator ORDER BY unit_price_per_qty NULLS LAST, id)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        operator,
        unit_price_per_qty,
        warehouse_wh_code,
        last_known_moved_at,
        last_known_unit_price_per_qty,
        distinct_sku_id_in_warehouse_id_move_type,
        unit_price_per_qty_cume_dist,
        unit_price_per_qty_share_of_moved_at_month_wareh,
        distinct_move_type_in_moved_at_month_warehouse_i,
        warehouse_id_move_type_min_unit_price_per_qty,
        warehouse_id_move_type_avg_unit_price_per_qty,
        CASE WHEN last_unit_price_per_qty >= 8.5 THEN 'A' WHEN last_unit_price_per_qty >= 2.5 THEN 'B' ELSE 'C' END AS last_unit_price_per_qty_tier
    FROM sequenced
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        last_known_moved_at,
        last_known_unit_price_per_qty,
        distinct_sku_id_in_warehouse_id_move_type,
        warehouse_id_move_type_min_unit_price_per_qty,
        last_unit_price_per_qty_tier,
        LAST_VALUE(moved_at) OVER (PARTITION BY MONTH(last_known_moved_at), last_unit_price_per_qty_tier ORDER BY last_known_moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        MAX(unit_price_per_qty_cume_dist) OVER (PARTITION BY operator) AS operator_max_unit_price_per_qty_cume_dist,
        ROUND(unit_price_per_qty - AVG(unit_price_per_qty) OVER (PARTITION BY operator), 2) AS unit_price_per_qty_vs_operator_avg,
        ROW_NUMBER() OVER (PARTITION BY MONTH(last_known_moved_at), last_unit_price_per_qty_tier ORDER BY last_known_moved_at DESC, id) AS seq_no,
        RANK() OVER w_move_type_by_unit_price_per AS unit_price_per_qty_rank,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY unit_price_per_qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at
    FROM derived
    WINDOW w_move_type_by_unit_price_per AS (PARTITION BY move_type ORDER BY unit_price_per_qty DESC, id)
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', first_known_moved_at) AS first_known_moved_at_month,
        ROUND(100 * COUNT(CASE WHEN move_type IN ('OUTBOUND', 'ADJUST') THEN 1 END) / COUNT(*), 1) AS pct_move_type_in_outbound_adjust,
        SUM(seq_no) AS total_seq_no,
        PERCENTILE_APPROX(operator_max_unit_price_per_qty_cume_dist, 0.5) AS median_operator_max_unit_price_per_qty_cume_dist,
        MIN(warehouse_id_move_type_min_unit_price_per_qty) AS min_warehouse_id_move_type_min_unit_price_per_qt,
        SUM(unit_price_per_qty_vs_operator_avg) AS total_unit_price_per_qty_vs_operator_avg
    FROM metrics
    GROUP BY 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        first_known_moved_at_month,
        pct_move_type_in_outbound_adjust,
        total_seq_no,
        median_operator_max_unit_price_per_qty_cume_dist,
        NTH_VALUE(total_seq_no, 2) OVER (ORDER BY pct_move_type_in_outbound_adjust ASC, first_known_moved_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_seq_no,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(first_known_moved_at_month) ORDER BY median_operator_max_unit_price_per_qty_cume_dist ASC), 3) AS median_operator_max_unit_price_per_qty_cume_dist_2,
        ROW_NUMBER() OVER (PARTITION BY MONTH(first_known_moved_at_month) ORDER BY median_operator_max_unit_price_per_qty_cume_dist ASC, first_known_moved_at_month) AS seq_no
    FROM per_group
)
SELECT
    first_known_moved_at_month,
    pct_move_type_in_outbound_adjust,
    median_operator_max_unit_price_per_qty_cume_dist,
    second_total_seq_no,
    seq_no,
    COALESCE(second_total_seq_no, 0) AS second_total_seq_no_filled,
    LEAST(second_total_seq_no, seq_no) AS least_second_total_seq_no_seq_no
FROM windowed
ORDER BY pct_move_type_in_outbound_adjust NULLS LAST, second_total_seq_no_filled NULLS LAST
