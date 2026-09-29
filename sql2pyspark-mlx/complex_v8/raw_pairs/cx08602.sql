WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price - qty AS unit_price_minus_qty,
        LOWER(TRIM(move_type)) AS move_type_clean,
        CONCAT(operator, '#', CAST(sku_id AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE operator = 'op_bee'
),
deduped AS (
    -- Keep the earliest row per operator, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price_minus_qty,
        move_type_clean,
        operator_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, move_type ORDER BY unit_price_minus_qty, id) AS recency_rank
        FROM base_moves
    ) ranked
    WHERE recency_rank = 1
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, MIN(moved_at) AS first_moved_at
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at
    FROM deduped s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        move_type,
        moved_at,
        operator,
        unit_price,
        move_type_clean,
        operator_ref,
        first_moved_at,
        FIRST_VALUE(unit_price) OVER (PARTITION BY move_type, operator ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        ROUND(unit_price_minus_qty / NULLIF(SUM(unit_price_minus_qty) OVER w_warehouse_id, 0), 4) AS unit_price_minus_qty_share_of_warehouse_id,
        MAX(qty) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_qty,
        ROUND(qty - AVG(qty) OVER (PARTITION BY warehouse_id), 2) AS qty_vs_warehouse_id_avg,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(moved_at) ORDER BY moved_at NULLS LAST, id) AS seq_no
    FROM profiled
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM sequenced
    WHERE qty_vs_warehouse_id_avg < (SELECT MAX(qty_vs_warehouse_id_avg) FROM sequenced)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_activity
    WHERE seq_no = 1
      AND move_type = 'INBOUND'
)
-- Final projection
SELECT
    id,
    moved_at,
    operator,
    move_type_clean,
    first_unit_price,
    unit_price_minus_qty_share_of_warehouse_id,
    warehouse_id_max_qty,
    seq_no,
    DAYOFWEEK(moved_at) AS moved_at_dayofweek,
    CASE WHEN seq_no >= 7 THEN 'HIGH' WHEN seq_no >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS seq_no_tier
FROM top_rows
ORDER BY seq_no_tier DESC
