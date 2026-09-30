WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj,
        CONCAT_WS(': ', operator, move_type) AS operator_move_type_label,
        LOWER(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE move_type NOT IN ('INBOUND', 'OUTBOUND')
      AND qty > 189
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(unit_price_adj, 1, 0) OVER w_all_by_unit_price AS next_unit_price_adj,
        ROW_NUMBER() OVER (ORDER BY unit_price_adj ASC, id) AS rn,
        FIRST_VALUE(unit_price) OVER (ORDER BY unit_price_adj ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        COUNT(*) OVER (PARTITION BY warehouse_id, move_type) AS rows_in_warehouse_id_move_type,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY warehouse_id, move_type)) AS distinct_operator_in_warehouse_id_move_type
    FROM qualified
    WINDOW w_all_by_unit_price AS (ORDER BY unit_price DESC, id)
),
with_parent_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.unit_price_adj,
        s.operator_move_type_label,
        s.next_unit_price_adj,
        s.rn,
        s.first_unit_price,
        s.rows_in_warehouse_id_move_type,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code
    FROM sequenced s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
grouped AS (
    -- Group-level totals
    SELECT
        rn,
        sku_id,
        warehouse_id,
        MIN(warehouse_opened_on) AS first_warehouse_opened_on,
        ROUND(SUM(warehouse_capacity) / NULLIF(SUM(rn), 0), 3) AS warehouse_capacity_to_rn_ratio
    FROM with_parent_warehouse
    GROUP BY 1, 2, 3
    HAVING AVG(unit_price_adj) > 300.6
)
-- Subtotals with GROUPING SETS
SELECT
    rn,
    sku_id,
    warehouse_id,
    MIN(first_warehouse_opened_on) AS first_first_warehouse_opened_on
FROM grouped
GROUP BY GROUPING SETS ((rn, sku_id, warehouse_id), (rn), (sku_id), ())
