WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(unit_price) OVER w_warehouse_id, 2) AS warehouse_id_avg_unit_price,
        RANK() OVER (PARTITION BY warehouse_id, operator ORDER BY unit_price) AS unit_price_rank
    FROM valid_moves
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id)
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id IS NOT NULL
              AND st.unit_price BETWEEN 58.6 AND 399.6
        )
      AND s.unit_price_rank <= 9
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM screened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND MONTH(st.moved_at) IN (1, 2)
        )
      AND s.warehouse_id <= 4
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        warehouse_id_avg_unit_price
    FROM qualified
    WHERE unit_price_rank <= 3
),
derived AS (
    -- Add calculated columns
    SELECT *, INITCAP(TRIM(operator)) AS operator_clean
    FROM top_per_group
    WHERE sku_id >= 7
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        warehouse_id_avg_unit_price,
        operator_clean,
        LENGTH(TRIM(operator_clean)) AS operator_clean_len
    FROM derived
    WHERE warehouse_id >= 4
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    warehouse_id_avg_unit_price,
    operator_clean,
    operator_clean_len,
    TO_DATE(moved_at) AS moved_at_date
FROM scored
ORDER BY operator_clean_len
