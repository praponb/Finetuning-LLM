WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket_2,
        DATE_TRUNC('MONTH', opened_on) AS opened_on_month
    FROM warehouses
    WHERE opened_on < DATE '2025-02-14'
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.city,
        s.capacity,
        s.capacity_bucket,
        s.capacity_bucket_2,
        s.opened_on_month,
        st.id AS move_id,
        st.move_type,
        st.qty AS move_qty
    FROM cleaned s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM with_children s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.operator IS NOT NULL
        )
),
latest_per_key AS (
    -- Keep the latest row per city
    SELECT
        id,
        wh_code,
        city,
        capacity,
        capacity_bucket,
        capacity_bucket_2,
        opened_on_month,
        move_id,
        move_type,
        move_qty
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY capacity_bucket DESC, id DESC, move_id DESC) AS row_num
        FROM qualified
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    wh_code,
    capacity_bucket_2,
    opened_on_month,
    move_id,
    move_type,
    DATEDIFF(DATE '2026-01-01', opened_on_month) AS days_since_opened_on_month,
    COALESCE(move_qty, 0) AS move_qty_filled
FROM latest_per_key
ORDER BY capacity_bucket_2 NULLS LAST
