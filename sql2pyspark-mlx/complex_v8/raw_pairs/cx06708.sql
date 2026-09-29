WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE unit_price > 112.8
      AND qty > 198
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_moved_at, 3) AS moved_at_percent_rank,
        DENSE_RANK() OVER (ORDER BY moved_at) AS moved_at_dense_rank,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type), 2) AS move_type_avg_unit_price,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type), 2) AS unit_price_vs_move_type_avg
    FROM scoped_moves
    WINDOW w_all_by_moved_at AS (ORDER BY moved_at, id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        warehouse_id,
        sku_id,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        MIN(moved_at) AS first_moved_at,
        COUNT(*) AS n_rows,
        ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM ranked
    WHERE move_type = 'OUTBOUND'
    GROUP BY 1, 2
    HAVING COUNT(*) >= 3
),
ranked_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(distinct_sku_ids) OVER (PARTITION BY distinct_sku_ids, sku_id ORDER BY n_rows, warehouse_id, sku_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS distinct_sku_ids_avg_4,
        NTH_VALUE(n_rows, 2) OVER (ORDER BY first_moved_at DESC, warehouse_id, sku_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_rows,
        NTILE(10) OVER (PARTITION BY n_rows, warehouse_id ORDER BY first_moved_at ASC, warehouse_id, sku_id) AS first_moved_at_decile,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id, distinct_sku_ids)) AS distinct_sku_id_in_warehouse_id_distinct_sku_ids,
        RANK() OVER (PARTITION BY distinct_sku_ids, sku_id ORDER BY n_rows, warehouse_id, sku_id) AS n_rows_rank
    FROM rollup_base
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.warehouse_id,
        s.sku_id,
        s.n_rows,
        s.avg_unit_price,
        s.second_n_rows,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price,
        st.warehouse_id AS move_warehouse_id
    FROM ranked_2 s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
-- Subtotals with GROUPING SETS
SELECT
    sku_id,
    warehouse_id,
    move_id,
    COUNT(*) AS cnt
FROM detail_rows_moves
GROUP BY GROUPING SETS ((sku_id, warehouse_id, move_id), (sku_id), ())
ORDER BY sku_id NULLS LAST, warehouse_id NULLS LAST, move_id NULLS LAST
