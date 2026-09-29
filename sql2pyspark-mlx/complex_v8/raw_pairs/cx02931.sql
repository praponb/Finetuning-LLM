WITH scoped AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
    WHERE operator IS NOT NULL
      AND qty BETWEEN 102 AND 357
),
screened AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND UPPER(TRIM(w.wh_code)) LIKE 'B%'
              AND w.capacity > 4912
        )
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        w.capacity AS warehouse_capacity
    FROM screened s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id AND w.wh_code IS NOT NULL
),
with_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.warehouse_capacity,
        w.capacity AS warehouse_capacity_2,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on
    FROM combined_details s
    JOIN warehouses w ON w.id = s.warehouse_id
),
screened_2 AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_details_warehouse s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id >= 3
        )
      AND s.warehouse_city <> 'Singapore'
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count_2
    FROM screened_2 s
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        warehouse_capacity,
        warehouse_capacity_2,
        warehouse_city,
        warehouse_opened_on,
        move_count,
        move_count_2,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY move_count)) AS distinct_move_type_in_move_count,
        MIN(move_count_2) OVER (ORDER BY warehouse_capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_move_count_2_so_far,
        LEAD(warehouse_capacity) OVER w_all_by_warehouse_capacity AS next_warehouse_capacity,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY move_count)) AS distinct_warehouse_id_in_move_count,
        MAX(move_count) OVER (PARTITION BY move_type ORDER BY move_count_2 ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS move_count_max_2_prior,
        SUM(move_count) OVER (ORDER BY warehouse_capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_move_count
    FROM summarized_rows
    WINDOW w_all_by_warehouse_capacity AS (ORDER BY warehouse_capacity, id)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    warehouse_capacity,
    warehouse_capacity_2,
    warehouse_city,
    move_count,
    move_count_2,
    distinct_move_type_in_move_count,
    min_move_count_2_so_far,
    distinct_warehouse_id_in_move_count,
    move_count_max_2_prior
FROM with_windows
ORDER BY sku_id
