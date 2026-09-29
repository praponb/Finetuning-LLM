WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour
    FROM stock_moves
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        moved_at_hour,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_price ASC, id), 3) AS unit_price_percent_rank,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY TO_DATE(moved_at_hour)), 2) AS unit_price_vs_moved_at_hour_to_date_avg,
        ROUND(AVG(unit_price) OVER w_moved_at_hour_to_date, 2) AS moved_at_hour_to_date_avg_unit_price,
        ROUND(CUME_DIST() OVER w_all_by_unit_price, 3) AS unit_price_cume_dist,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY TO_DATE(moved_at_hour))) AS distinct_warehouse_id_in_moved_at_hour_to_date,
        ROW_NUMBER() OVER (ORDER BY unit_price ASC, id) AS seq_no
    FROM valid_moves
    WINDOW w_moved_at_hour_to_date AS (PARTITION BY TO_DATE(moved_at_hour)), w_all_by_unit_price AS (ORDER BY unit_price ASC, id)
),
latest AS (
    -- Keep the earliest row per move_type, warehouse_id
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        moved_at_hour,
        unit_price_percent_rank,
        unit_price_vs_moved_at_hour_to_date_avg,
        moved_at_hour_to_date_avg_unit_price,
        unit_price_cume_dist,
        distinct_warehouse_id_in_moved_at_hour_to_date,
        seq_no
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type, warehouse_id ORDER BY moved_at_hour_to_date_avg_unit_price, id) AS rn
        FROM ranked
    ) ranked
    WHERE rn = 1
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.move_type,
        s.moved_at,
        s.unit_price_percent_rank,
        s.moved_at_hour_to_date_avg_unit_price,
        s.unit_price_cume_dist,
        s.distinct_warehouse_id_in_moved_at_hour_to_date,
        s.seq_no,
        st.id AS move_id,
        st.qty AS move_qty,
        st.move_type AS move_move_type
    FROM latest s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(unit_price_percent_rank - 0.4) AS unit_price_percent_rank_distance,
        ROUND(unit_price_cume_dist * 0.25, 2) AS unit_price_cume_dist_adj,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM with_children
    WHERE move_type = 'OUTBOUND'
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month
    FROM enriched
)
-- Final projection
SELECT
    id,
    move_type,
    moved_at_hour_to_date_avg_unit_price,
    unit_price_cume_dist,
    distinct_warehouse_id_in_moved_at_hour_to_date,
    move_id,
    move_qty,
    unit_price_percent_rank_distance,
    move_type_len,
    move_type_slug
FROM labeled
ORDER BY unit_price_cume_dist DESC
