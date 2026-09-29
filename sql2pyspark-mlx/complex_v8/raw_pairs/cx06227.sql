WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator = 'op_dew'
      AND unit_price BETWEEN 152.7 AND 398.5
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        sk.labels AS sku_labels
    FROM base s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
deduped AS (
    -- Keep the latest row per operator, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        sku_labels
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, move_type ORDER BY qty DESC, id DESC) AS recency_rank
        FROM combined_details
    ) ranked
    WHERE recency_rank = 1
),
exploded AS (
    -- One row per sku_label
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        t.sku_label
    FROM deduped s
    LATERAL VIEW EXPLODE(s.sku_labels) t AS sku_label
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city
    FROM exploded s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.capacity >= 2614
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS min_move_qty,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at_2
    FROM joined s
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.sku_label,
        s.min_move_qty,
        s.last_moved_at_2,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator
    FROM summarized_rows s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    sku_label,
    min_move_qty,
    last_moved_at_2,
    move_unit_price,
    move_operator,
    ROUND(move_unit_price * 1.07, 2) AS move_unit_price_adj
FROM exploded_detail
WHERE min_move_qty > 226
ORDER BY sku_label NULLS LAST
