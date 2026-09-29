WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        moved_at + INTERVAL 7 DAYS AS moved_at_plus_7d,
        unit_price + qty AS unit_price_plus_qty
    FROM stock_moves
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(qty) IGNORE NULLS OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        ROW_NUMBER() OVER w_sku_id_by_unit_price_plus_qt AS unit_price_plus_qty_seq,
        LAST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY sku_id ORDER BY unit_price_plus_qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty_2,
        NTH_VALUE(unit_price_plus_qty, 2) OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_plus_qty,
        LEAD(qty, 2) OVER w_all_by_unit_price AS next2_qty,
        ROUND(unit_price_plus_qty - AVG(unit_price_plus_qty) OVER (PARTITION BY warehouse_id), 2) AS unit_price_plus_qty_vs_warehouse_id_avg,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        COUNT(*) OVER w_warehouse_id AS rows_in_warehouse_id,
        unit_price - LAG(unit_price) OVER (ORDER BY unit_price, id) AS unit_price_change,
        MAX(qty) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_qty,
        ROUND(AVG(qty) OVER (ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS qty_avg_3,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id), 2) AS unit_price_vs_warehouse_id_avg
    FROM prepared
    WINDOW w_sku_id_by_unit_price_plus_qt AS (PARTITION BY sku_id ORDER BY unit_price_plus_qty ASC, id), w_all_by_unit_price AS (ORDER BY unit_price ASC, id), w_warehouse_id AS (PARTITION BY warehouse_id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_trends
    WHERE unit_price_plus_qty_seq = 1
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(last_known_unit_price) OVER (ORDER BY moved_at_plus_7d, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS last_known_unit_price_max_2_prior,
        LAG(last_known_unit_price) OVER (ORDER BY next2_qty DESC, id) AS prev_last_known_unit_price,
        ROW_NUMBER() OVER (ORDER BY moved_at_plus_7d, id) AS seq_no
    FROM top_rows
),
stats AS (
    -- Group-level totals
    SELECT
        TO_DATE(moved_at_plus_7d) AS moved_at_plus_7d_day,
        warehouse_id,
        unit_price_plus_qty_seq,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        COUNT(*) AS row_count
    FROM with_windows
    GROUP BY TO_DATE(moved_at_plus_7d), warehouse_id, unit_price_plus_qty_seq
    HAVING COUNT(*) >= 2
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY row_count ORDER BY row_count DESC, moved_at_plus_7d_day, warehouse_id, unit_price_plus_qty_seq) AS row_count_decile,
        ROW_NUMBER() OVER (ORDER BY row_count ASC, moved_at_plus_7d_day, warehouse_id, unit_price_plus_qty_seq) AS seq_no,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY unit_price_plus_qty_seq)) AS distinct_warehouse_id_in_unit_price_plus_qty_seq
    FROM stats
)
SELECT
    moved_at_plus_7d_day,
    warehouse_id,
    distinct_warehouse_ids,
    row_count_decile,
    seq_no,
    distinct_warehouse_id_in_unit_price_plus_qty_seq,
    TRUNC(moved_at_plus_7d_day, 'MM') AS moved_at_plus_7d_day_month_start
FROM sequenced
ORDER BY row_count_decile
