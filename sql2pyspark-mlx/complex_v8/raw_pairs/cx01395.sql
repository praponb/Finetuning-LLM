WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        ROUND(unit_price * 1.5, 2) AS unit_price_adj
    FROM stock_moves
    WHERE sku_id >= 10
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(unit_price_adj) OVER w_move_type_sku_id, 2) AS move_type_sku_id_avg_unit_price_adj,
        NTILE(3) OVER (ORDER BY unit_price_adj DESC, id) AS unit_price_adj_tercile,
        ROUND(AVG(unit_price) OVER (ORDER BY unit_price_adj DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_3_prior,
        ROUND(CUME_DIST() OVER (ORDER BY unit_price_adj DESC, id), 3) AS unit_price_adj_cume_dist,
        DENSE_RANK() OVER (ORDER BY unit_price_adj DESC) AS unit_price_adj_dense_rank
    FROM cleaned_moves
    WINDOW w_move_type_sku_id AS (PARTITION BY move_type, sku_id)
),
with_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.unit_price_adj,
        s.move_type_sku_id_avg_unit_price_adj,
        s.unit_price_avg_3_prior,
        s.unit_price_adj_cume_dist,
        s.unit_price_adj_dense_rank,
        w.capacity AS warehouse_capacity
    FROM ranked s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_details_warehouse s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.operator = 'op_bee'
              AND st.qty BETWEEN 47 AND 393
        )
      AND s.unit_price < 277.6
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM with_activity
    WHERE unit_price_adj_dense_rank <= 3
      AND sku_id >= 14
)
SELECT
    id,
    warehouse_id,
    sku_id,
    unit_price,
    unit_price_adj,
    unit_price_adj_dense_rank,
    warehouse_capacity
FROM leaders
WHERE unit_price_avg_3_prior <= 400.2
