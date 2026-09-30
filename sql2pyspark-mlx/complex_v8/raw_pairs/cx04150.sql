WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        LOWER(TRIM(move_type)) AS move_type_clean,
        COALESCE(qty, 0) AS qty_filled
    FROM stock_moves
    WHERE unit_price > 119.5
      AND operator IS NOT NULL
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id IS NOT NULL
        )
),
combined_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_clean,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on
    FROM without_matches s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator
    FROM combined_details_warehouse
),
summary AS (
    -- Summarise per group
    SELECT
        warehouse_city,
        DATE_TRUNC('MONTH', warehouse_opened_on) AS warehouse_opened_on_month,
        operator,
        COUNT(*) AS row_count,
        SUM(CASE WHEN UPPER(TRIM(warehouse_wh_code)) LIKE 'C%' THEN 1 ELSE 0 END) AS n_upper_trim_warehouse_wh_code,
        MIN(rows_in_operator) AS min_rows_in_operator,
        COUNT(DISTINCT id) AS distinct_ids
    FROM ranked
    GROUP BY 1, 2, 3
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        warehouse_city,
        warehouse_opened_on_month,
        operator,
        row_count,
        n_upper_trim_warehouse_wh_code,
        min_rows_in_operator,
        distinct_ids,
        ROUND(min_rows_in_operator - AVG(min_rows_in_operator) OVER (PARTITION BY row_count), 2) AS min_rows_in_operator_vs_row_count_avg,
        ROUND(distinct_ids - AVG(distinct_ids) OVER w_row_count, 2) AS distinct_ids_vs_row_count_avg,
        row_count - LAG(row_count) OVER (PARTITION BY warehouse_city, row_count ORDER BY distinct_ids DESC NULLS FIRST, warehouse_city, warehouse_opened_on_month, operator) AS row_count_change
    FROM summary
    WINDOW w_row_count AS (PARTITION BY row_count)
)
-- Final projection
SELECT
    warehouse_city,
    row_count,
    n_upper_trim_warehouse_wh_code,
    min_rows_in_operator,
    distinct_ids,
    min_rows_in_operator_vs_row_count_avg,
    distinct_ids_vs_row_count_avg,
    row_count_change
FROM ranked_2
ORDER BY row_count_change DESC
