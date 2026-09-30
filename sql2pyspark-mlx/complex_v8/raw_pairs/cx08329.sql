WITH valid AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE (qty > 145 OR warehouse_id IS NOT NULL)
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
        s.unit_price,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city
    FROM valid s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id AND w.opened_on < DATE '2025-02-14'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        qty,
        moved_at,
        warehouse_opened_on,
        warehouse_city,
        RANK() OVER w_all_by_warehouse_opened_on AS warehouse_opened_on_rank,
        MAX(unit_price) OVER (PARTITION BY warehouse_city) AS warehouse_city_max_unit_price,
        LAG(moved_at, 3) OVER (ORDER BY unit_price, id) AS prev3_moved_at,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY warehouse_opened_on ASC, id) AS warehouse_opened_on_quintile,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY MONTH(moved_at)), 0), 4) AS unit_price_share_of_moved_at_month,
        SUM(unit_price) OVER w_move_type AS move_type_sum_unit_price,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY warehouse_opened_on ASC, id) AS warehouse_opened_on_rank_2,
        MIN(qty) OVER (PARTITION BY MONTH(moved_at)) AS moved_at_month_min_qty,
        LAG(moved_at) OVER w_warehouse_city_warehouse_id AS prev_moved_at,
        MAX(qty) OVER (PARTITION BY warehouse_city, warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_qty_so_far,
        NTILE(3) OVER (PARTITION BY warehouse_city, warehouse_id ORDER BY unit_price DESC, id) AS unit_price_tercile,
        COUNT(*) OVER w_warehouse_city AS rows_in_warehouse_city,
        LAG(warehouse_opened_on) OVER (PARTITION BY sku_id ORDER BY unit_price, id) AS prev_warehouse_opened_on
    FROM combined_details
    WINDOW w_all_by_warehouse_opened_on AS (ORDER BY warehouse_opened_on, id), w_move_type AS (PARTITION BY move_type), w_warehouse_city_warehouse_id AS (PARTITION BY warehouse_city, warehouse_id ORDER BY unit_price DESC, id), w_warehouse_city AS (PARTITION BY warehouse_city)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        qty,
        moved_at,
        warehouse_opened_on,
        warehouse_city,
        warehouse_opened_on_rank,
        warehouse_city_max_unit_price,
        prev3_moved_at,
        warehouse_opened_on_quintile,
        unit_price_share_of_moved_at_month,
        warehouse_opened_on_rank_2,
        moved_at_month_min_qty,
        prev_moved_at,
        max_qty_so_far,
        rows_in_warehouse_city,
        prev_warehouse_opened_on,
        RANK() OVER (PARTITION BY sku_id, warehouse_opened_on_rank_2 ORDER BY prev3_moved_at DESC) AS prev3_moved_at_rank,
        move_type_sum_unit_price - LAG(move_type_sum_unit_price) OVER (PARTITION BY sku_id, warehouse_opened_on_rank_2 ORDER BY prev3_moved_at DESC, id) AS move_type_sum_unit_price_change,
        MAX(warehouse_opened_on_rank_2) OVER (PARTITION BY sku_id, warehouse_opened_on_rank_2 ORDER BY prev3_moved_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS warehouse_opened_on_rank_2_max_3,
        ROW_NUMBER() OVER w_warehouse_city_by_warehouse AS seq_no
    FROM ranked
    WINDOW w_warehouse_city_by_warehouse AS (PARTITION BY warehouse_city ORDER BY warehouse_city_max_unit_price, id)
)
SELECT
    id,
    qty,
    moved_at,
    warehouse_opened_on,
    warehouse_city_max_unit_price,
    prev3_moved_at,
    warehouse_opened_on_rank_2,
    moved_at_month_min_qty,
    prev_moved_at,
    max_qty_so_far,
    rows_in_warehouse_city,
    prev3_moved_at_rank,
    warehouse_opened_on_rank_2_max_3,
    seq_no,
    FLOOR(warehouse_opened_on_rank / 2) * 2 AS warehouse_opened_on_rank_bucket,
    CASE WHEN warehouse_city IN ('Singapore', 'Kuala Lumpur') THEN 'group_a' WHEN warehouse_city = 'Bangkok' THEN 'group_b' ELSE 'other' END AS warehouse_city_group
FROM with_windows
WHERE unit_price_share_of_moved_at_month < 0.9
ORDER BY warehouse_city_group DESC, prev3_moved_at_rank DESC
