WITH cleaned_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN operator IN ('op_ann', 'op_eve') THEN 'group_a' WHEN operator = 'op_dew' THEN 'group_b' ELSE 'other' END AS operator_group,
        SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE (unit_price > 200.7 OR move_type = 'ADJUST')
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', moved_at_date) AS moved_at_date_quarter,
        ABS(qty - 230) AS qty_distance
    FROM cleaned_moves
),
enriched_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.operator_group,
        s.operator_prefix,
        s.moved_at_date,
        s.moved_at_date_quarter,
        s.qty_distance,
        w.wh_code AS warehouse_wh_code
    FROM tagged s
    JOIN warehouses w ON w.id = s.warehouse_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        warehouse_id,
        ROUND(SUM(unit_price) / NULLIF(SUM(qty_distance), 0), 3) AS unit_price_to_qty_distance_ratio,
        MIN(moved_at_date) AS first_moved_at_date,
        ARRAY_SORT(COLLECT_SET(warehouse_wh_code)) AS warehouse_wh_code_list,
        SUM(qty) AS total_qty
    FROM enriched_warehouse
    GROUP BY warehouse_id
    HAVING SUM(unit_price) > 86.3
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        total_qty - LAG(total_qty) OVER w_first_moved_at_date_month_wa AS total_qty_change,
        ROUND(total_qty / NULLIF(SUM(total_qty) OVER w_warehouse_id, 0), 4) AS total_qty_share_of_warehouse_id
    FROM rollup_base
    WINDOW w_first_moved_at_date_month_wa AS (PARTITION BY DATE_TRUNC('MONTH', first_moved_at_date), warehouse_id ORDER BY total_qty ASC, warehouse_id), w_warehouse_id AS (PARTITION BY warehouse_id)
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.warehouse_id,
        s.unit_price_to_qty_distance_ratio,
        s.total_qty_change,
        s.total_qty_share_of_warehouse_id,
        st.id AS move_id,
        st.move_type,
        st.unit_price AS move_unit_price,
        st.sku_id AS move_sku_id,
        st.operator AS move_operator
    FROM sequenced s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.sku_id >= 10
),
latest_per_key AS (
    -- Keep the latest row per move_operator
    SELECT
        warehouse_id,
        unit_price_to_qty_distance_ratio,
        total_qty_change,
        total_qty_share_of_warehouse_id,
        move_id,
        move_type,
        move_unit_price,
        move_sku_id,
        move_operator
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_operator ORDER BY total_qty_share_of_warehouse_id DESC, warehouse_id DESC, move_id DESC) AS recency_rank
        FROM exploded_detail
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    warehouse_id,
    unit_price_to_qty_distance_ratio,
    total_qty_change,
    total_qty_share_of_warehouse_id,
    move_id,
    move_type,
    move_unit_price,
    move_sku_id,
    move_operator
FROM latest_per_key
