WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix,
        SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix_2
    FROM stock_moves
    WHERE operator NOT IN ('op_dew', 'op_chan')
),
latest_per_key AS (
    -- Keep the latest row per operator, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        unit_price_bucket,
        move_type_prefix,
        move_type_prefix_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, sku_id ORDER BY unit_price_bucket DESC, id DESC) AS row_num
        FROM scoped
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Label and score rows
    SELECT *, DATE_TRUNC('DAY', moved_at) AS moved_at_day
    FROM latest_per_key
    WHERE unit_price_bucket > 76
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN operator IN ('op_bee', 'op_dew') THEN 'group_a' WHEN operator = 'op_eve' THEN 'group_b' ELSE 'other' END AS operator_group,
        COALESCE(unit_price_bucket, 0) AS unit_price_bucket_filled,
        LENGTH(TRIM(move_type_prefix)) AS move_type_prefix_len
    FROM enriched
    WHERE unit_price <= 329.7
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.unit_price) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS max_move_unit_price
    FROM labeled s
),
move_stats_2 AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        ROUND(AVG(unit_price), 2) AS avg_move_unit_price,
        MAX(unit_price) AS max_move_unit_price_2
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_unit_price,
        st.max_move_unit_price_2
    FROM with_stats s
    LEFT JOIN move_stats_2 st ON st.warehouse_id = s.warehouse_id
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    unit_price,
    unit_price_bucket,
    move_type_prefix,
    unit_price_bucket_filled,
    move_type_prefix_len,
    max_move_unit_price,
    avg_move_unit_price,
    max_move_unit_price_2
FROM with_counts
