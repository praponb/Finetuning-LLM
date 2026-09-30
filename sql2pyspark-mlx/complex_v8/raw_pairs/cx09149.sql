WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id <= 2
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_unit_price_ties AS unit_price_dense_rank,
        MIN(unit_price) OVER (PARTITION BY warehouse_id) AS warehouse_id_min_unit_price,
        MAX(unit_price) OVER w_warehouse_id AS warehouse_id_max_unit_price,
        NTILE(5) OVER (ORDER BY unit_price DESC, id) AS unit_price_quintile
    FROM prepared
    WINDOW w_all_by_unit_price_ties AS (ORDER BY unit_price DESC), w_warehouse_id AS (PARTITION BY warehouse_id)
),
unique_rows AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        unit_price_dense_rank,
        warehouse_id_min_unit_price,
        warehouse_id_max_unit_price,
        unit_price_quintile
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY warehouse_id_min_unit_price DESC, id DESC) AS recency_rank
        FROM metrics
    ) ranked
    WHERE recency_rank = 1
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at,
        (SELECT COUNT(*) FILTER (WHERE st.move_type = 'TRANSFER') FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_move_type_count
    FROM unique_rows s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT unit_price_quintile, operator, warehouse_id_min_unit_price
    FROM summarized_rows
) src
PIVOT (
    MIN(warehouse_id_min_unit_price)
    FOR operator IN ('op_ann', 'op_eve', 'op_bee', 'op_dew')
)
ORDER BY unit_price_quintile
