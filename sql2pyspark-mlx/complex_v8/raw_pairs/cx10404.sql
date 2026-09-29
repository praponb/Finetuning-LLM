WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        LEAST(qty, unit_price) AS least_qty_unit_price
    FROM stock_moves
),
deduped AS (
    -- Keep the latest row per operator, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        least_qty_unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, sku_id ORDER BY unit_price DESC, id DESC) AS row_num
        FROM cleaned_moves
    ) ranked
    WHERE row_num = 1
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM deduped s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.sku_id IS NOT NULL
              AND st.unit_price > 215.4
        )
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    least_qty_unit_price,
    CONCAT(operator, '#', CAST(qty AS STRING)) AS operator_ref,
    ROUND(least_qty_unit_price / NULLIF(unit_price, 0), 3) AS least_qty_unit_price_per_unit_price
FROM screened
