WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
    WHERE qty > 169
      AND move_type NOT IN ('ADJUST', 'OUTBOUND')
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek,
        CONCAT_WS(': ', operator, operator_or_unknown) AS operator_operator_or_unknown_label
    FROM prepared_moves
),
latest AS (
    -- Keep the latest row per operator_or_unknown
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        operator_or_unknown,
        qty_per_unit_price,
        moved_at_dayofweek,
        operator_operator_or_unknown_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator_or_unknown ORDER BY qty DESC, id DESC) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id IS NOT NULL
              AND st.unit_price > 206.8
        )
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        sku_id,
        warehouse_id,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        ROUND(SUM(qty) / NULLIF(SUM(moved_at_dayofweek), 0), 3) AS qty_to_moved_at_dayofweek_ratio,
        SUM(CASE WHEN operator = 'op_ann' THEN moved_at_dayofweek ELSE 0 END) AS moved_at_dayofweek_operator_op_ann,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(qty) AS total_qty,
        MAX(moved_at_dayofweek) AS max_moved_at_dayofweek
    FROM with_activity
    GROUP BY 1, 2, 3
    HAVING MAX(qty) > 169
)
-- Subtotals with ROLLUP
SELECT
    warehouse_id,
    max_moved_at_dayofweek,
    sku_id,
    GROUPING(warehouse_id) AS is_total_warehouse_id,
    PERCENTILE_APPROX(max_moved_at_dayofweek, 0.5) AS median_max_moved_at_dayofweek,
    MIN(moved_at_month) AS first_moved_at_month,
    ROUND(STDDEV(max_moved_at_dayofweek), 2) AS stddev_max_moved_at_dayofweek
FROM grouped
GROUP BY ROLLUP(warehouse_id, max_moved_at_dayofweek, sku_id)
ORDER BY warehouse_id NULLS LAST, max_moved_at_dayofweek NULLS LAST, sku_id NULLS LAST
