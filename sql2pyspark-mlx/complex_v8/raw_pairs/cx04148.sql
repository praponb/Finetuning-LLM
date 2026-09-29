WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        weight_kg
    FROM skus
),
latest_per_key AS (
    -- Keep the latest row per category
    SELECT
        id,
        sku_code,
        category,
        weight_kg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY weight_kg DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.id) AS move_count
    FROM latest_per_key s
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN st.operator IN ('op_chan', 'op_eve', 'op_dew') THEN 1 END) FROM stock_moves st WHERE st.sku_id = s.id) AS move_operator_count,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.id) AS last_moved_at
    FROM with_counts s
),
with_counts_2 AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.id) AS last_moved_at_2,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.id) AS last_moved_at_3
    FROM profiled s
)
SELECT id, metric_name, amount
FROM (
    SELECT id, move_operator_count, move_count
    FROM with_counts_2
) src
UNPIVOT (
    amount FOR metric_name IN (move_operator_count, move_count)
) unp
