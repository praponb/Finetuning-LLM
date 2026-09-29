WITH base AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
    WHERE move_type IN ('ADJUST', 'INBOUND', 'TRANSFER')
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
        s.operator,
        s.unit_price,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city
    FROM base s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE UPPER(TRIM(w.wh_code)) LIKE 'K%'
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM combined_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.moved_at >= TIMESTAMP '2025-02-06 08:00:00'
        )
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS first_moved_at,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at
    FROM with_activity s
),
latest_per_key AS (
    -- Keep the earliest row per operator, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        warehouse_wh_code,
        warehouse_city,
        first_moved_at,
        last_moved_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, warehouse_id ORDER BY moved_at, id) AS rn
        FROM with_stats
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(qty) OVER (PARTITION BY warehouse_city, operator ORDER BY first_moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        ROW_NUMBER() OVER (PARTITION BY warehouse_city, operator ORDER BY first_moved_at DESC, id) AS seq_no,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY TO_DATE(first_moved_at)), 0), 4) AS unit_price_share_of_first_moved_at_to_date,
        LAST_VALUE(first_moved_at) OVER (PARTITION BY warehouse_city, operator ORDER BY first_moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_moved_at,
        ROUND(AVG(qty) OVER (PARTITION BY TO_DATE(first_moved_at)), 2) AS first_moved_at_to_date_avg_qty
    FROM latest_per_key
),
latest_per_key_2 AS (
    -- Keep the latest row per warehouse_city, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        warehouse_wh_code,
        warehouse_city,
        first_moved_at,
        last_moved_at,
        running_qty,
        seq_no,
        unit_price_share_of_first_moved_at_to_date,
        last_first_moved_at,
        first_moved_at_to_date_avg_qty
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_city, sku_id ORDER BY seq_no DESC, id DESC) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    unit_price,
    warehouse_city,
    first_moved_at,
    last_moved_at,
    seq_no,
    unit_price_share_of_first_moved_at_to_date,
    last_first_moved_at,
    first_moved_at_to_date_avg_qty
FROM latest_per_key_2
ORDER BY first_moved_at_to_date_avg_qty
