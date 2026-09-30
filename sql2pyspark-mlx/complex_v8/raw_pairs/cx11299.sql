WITH base AS (
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
        ABS(qty - 230) AS qty_distance,
        unit_price - qty AS unit_price_minus_qty
    FROM stock_moves
    WHERE unit_price >= 111.7
),
combined_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT s.*, w.opened_on AS warehouse_opened_on, w.wh_code AS warehouse_wh_code
    FROM base s
    JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        qty_distance,
        warehouse_wh_code,
        SUM(unit_price) OVER (PARTITION BY TO_DATE(moved_at)) AS moved_at_to_date_sum_unit_price,
        ROW_NUMBER() OVER (ORDER BY warehouse_opened_on DESC, id) AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id ORDER BY qty_distance), 3) AS qty_distance_percent_rank,
        NTH_VALUE(warehouse_opened_on, 2) OVER (PARTITION BY sku_id ORDER BY qty_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id ORDER BY qty_distance), 3) AS qty_distance_cume_dist,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY YEAR(warehouse_opened_on), sku_id), 0), 4) AS qty_share_of_warehouse_opened_on_year_sku_id,
        COUNT(*) OVER (PARTITION BY YEAR(warehouse_opened_on), sku_id) AS rows_in_warehouse_opened_on_year_sku_id,
        SIZE(COLLECT_SET(sku_id) OVER w_moved_at_to_date) AS distinct_sku_id_in_moved_at_to_date,
        NTH_VALUE(qty, 2) OVER (PARTITION BY operator ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty
    FROM combined_details_warehouse
    WINDOW w_warehouse_opened_on_year_sku AS (PARTITION BY YEAR(warehouse_opened_on), sku_id), w_moved_at_to_date AS (PARTITION BY TO_DATE(moved_at))
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        qty_distance,
        warehouse_wh_code,
        moved_at_to_date_sum_unit_price,
        qty_distance_percent_rank,
        second_warehouse_opened_on,
        qty_distance_cume_dist,
        qty_share_of_warehouse_opened_on_year_sku_id,
        rows_in_warehouse_opened_on_year_sku_id,
        distinct_sku_id_in_moved_at_to_date,
        second_qty
    FROM with_trends
    WHERE rn <= 3
      AND qty_distance > 75
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        qty_distance,
        moved_at_to_date_sum_unit_price,
        qty_distance_percent_rank,
        second_warehouse_opened_on,
        qty_distance_cume_dist,
        qty_share_of_warehouse_opened_on_year_sku_id,
        distinct_sku_id_in_moved_at_to_date,
        second_qty,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY MONTH(second_warehouse_opened_on), distinct_sku_id_in_moved_at_to_date)) AS distinct_sku_id_in_second_warehouse_opened_on_mo,
        SIZE(COLLECT_SET(warehouse_id) OVER w_second_warehouse_opened_on_m) AS distinct_warehouse_id_in_second_warehouse_opened,
        ROUND(qty_distance_percent_rank - AVG(qty_distance_percent_rank) OVER (PARTITION BY YEAR(moved_at), warehouse_id), 2) AS qty_distance_percent_rank_vs_moved_at_year_wareh,
        SUM(qty_distance_percent_rank) OVER (ORDER BY second_warehouse_opened_on DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty_distance_percent_rank,
        LAG(qty_distance_cume_dist, 2) OVER (PARTITION BY operator, distinct_sku_id_in_moved_at_to_date ORDER BY unit_price DESC, id) AS prev2_qty_distance_cume_dist,
        FIRST_VALUE(qty_distance_cume_dist) OVER (ORDER BY second_warehouse_opened_on DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty_distance_cume_dist
    FROM leaders
    WINDOW w_second_warehouse_opened_on_m AS (PARTITION BY MONTH(second_warehouse_opened_on), distinct_sku_id_in_moved_at_to_date)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    operator,
    unit_price,
    qty_distance,
    moved_at_to_date_sum_unit_price,
    qty_distance_percent_rank,
    qty_distance_cume_dist,
    qty_share_of_warehouse_opened_on_year_sku_id,
    second_qty,
    distinct_sku_id_in_second_warehouse_opened_on_mo,
    distinct_warehouse_id_in_second_warehouse_opened,
    running_qty_distance_percent_rank,
    prev2_qty_distance_cume_dist,
    first_qty_distance_cume_dist,
    CASE WHEN operator IN ('op_eve', 'op_ann') THEN 'group_a' WHEN operator = 'op_chan' THEN 'group_b' ELSE 'other' END AS operator_group
FROM with_windows
ORDER BY first_qty_distance_cume_dist DESC
