WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        ROUND(unit_price * 1.5, 2) AS unit_price_adj,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek
    FROM stock_moves
    WHERE (sku_id >= 14 OR MONTH(moved_at) IN (1, 3))
),
latest_per_key AS (
    -- Keep the earliest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        qty_bucket,
        unit_price_adj,
        moved_at_dayofweek
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY unit_price_adj, id) AS recency_rank
        FROM base
    ) ranked
    WHERE recency_rank = 1
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM latest_per_key s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
      AND s.moved_at_dayofweek <= 6
),
unique_rows AS (
    -- Keep the earliest row per move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        qty_bucket,
        unit_price_adj,
        moved_at_dayofweek
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY qty, id) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
)
SELECT
    COALESCE(move_type, 'ALL') AS move_type,
    sku_id,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(unit_price_adj), 2) AS avg_unit_price_adj
FROM unique_rows
GROUP BY GROUPING SETS ((move_type, sku_id), (move_type), (sku_id))
