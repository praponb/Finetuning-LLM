WITH scoped AS (
    -- Scope the warehouses rows
    SELECT
        id,
        city,
        capacity,
        opened_on,
        COALESCE(city, 'UNKNOWN') AS city_or_unknown,
        QUARTER(opened_on) AS opened_on_quarter,
        INITCAP(TRIM(wh_code)) AS wh_code_clean
    FROM warehouses
    WHERE opened_on < DATE '2025-07-03'
      AND city = 'Singapore'
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        city,
        opened_on,
        city_or_unknown,
        opened_on_quarter,
        wh_code_clean,
        COALESCE(capacity, 0) AS capacity_filled,
        CASE WHEN city_or_unknown IN ('Singapore', 'Bangkok') THEN 'group_a' WHEN city_or_unknown = 'Kuala Lumpur' THEN 'group_b' ELSE 'other' END AS city_or_unknown_group
    FROM scoped
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.city,
        s.opened_on,
        s.opened_on_quarter,
        s.wh_code_clean,
        st.id AS move_id,
        st.qty AS move_qty,
        st.sku_id AS move_sku_id,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator
    FROM scored s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        city,
        opened_on_quarter,
        wh_code_clean,
        move_id,
        move_qty,
        move_sku_id,
        move_moved_at,
        move_operator,
        MAX(opened_on_quarter) OVER (ORDER BY opened_on_quarter, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_opened_on_quarter_so_far,
        MIN(opened_on_quarter) OVER w_move_id_move_operator AS move_id_move_operator_min_opened_on_quarter,
        MAX(opened_on_quarter) OVER (ORDER BY opened_on_quarter, id, move_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS opened_on_quarter_max_3_prior,
        FIRST_VALUE(move_qty) OVER (ORDER BY opened_on_quarter, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_move_qty,
        MAX(opened_on_quarter) OVER w_move_id_move_operator AS move_id_move_operator_max_opened_on_quarter,
        ROW_NUMBER() OVER w_all_by_opened_on_quarter AS rn
    FROM exploded_detail
    WINDOW w_move_id_move_operator AS (PARTITION BY move_id, move_operator), w_all_by_opened_on_quarter AS (ORDER BY opened_on_quarter, id, move_id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        city,
        opened_on_quarter,
        move_id,
        move_sku_id,
        max_opened_on_quarter_so_far,
        move_id_move_operator_min_opened_on_quarter,
        opened_on_quarter_max_3_prior,
        first_move_qty,
        move_id_move_operator_max_opened_on_quarter,
        DENSE_RANK() OVER (PARTITION BY move_id, move_sku_id ORDER BY move_qty DESC) AS move_qty_dense_rank,
        ROUND(opened_on_quarter / NULLIF(SUM(opened_on_quarter) OVER (PARTITION BY move_id, city), 0), 4) AS opened_on_quarter_share_of_move_id_city,
        RANK() OVER (PARTITION BY move_id, move_sku_id ORDER BY move_qty DESC) AS move_qty_rank
    FROM ranked
)
-- Subtotals with ROLLUP
SELECT
    move_qty_rank,
    move_id,
    move_sku_id,
    SUM(max_opened_on_quarter_so_far) AS total_max_opened_on_quarter_so_far,
    COUNT(DISTINCT city) AS distinct_citys
FROM windowed
GROUP BY ROLLUP(move_qty_rank, move_id, move_sku_id)
