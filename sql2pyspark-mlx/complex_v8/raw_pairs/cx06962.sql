WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
    WHERE unit_price BETWEEN 111.7 AND 330.0
      AND qty > 148
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN operator IN ('op_dew', 'op_bee') THEN 'group_a' WHEN operator = 'op_chan' THEN 'group_b' ELSE 'other' END AS operator_group,
        ROUND(unit_price * 1.5, 2) AS unit_price_adj
    FROM recent_moves
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        LENGTH(TRIM(operator)) AS operator_len,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM derived
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY operator_len, warehouse_id ORDER BY operator_len ASC) AS operator_len_dense_rank,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        NTILE(10) OVER (PARTITION BY operator_len, warehouse_id ORDER BY operator_len ASC, id) AS operator_len_decile
    FROM scored
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.move_type_len,
        s.operator_group,
        s.unit_price_filled,
        s.operator_len,
        s.operator_or_unknown,
        s.operator_len_dense_rank,
        s.rows_in_sku_id,
        s.operator_len_decile,
        sk.weight_kg AS sku_weight_kg,
        sk.category AS sku_category,
        sk.sku_code
    FROM sequenced s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.weight_kg BETWEEN 6.5 AND 15.2
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.unit_price_filled,
        s.operator_len,
        s.operator_len_dense_rank,
        s.operator_len_decile,
        s.sku_weight_kg,
        s.sku_category,
        st.id AS move_id,
        st.operator AS move_operator,
        st.move_type AS move_move_type
    FROM joined s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    operator,
    unit_price,
    unit_price_filled,
    operator_len,
    operator_len_dense_rank,
    operator_len_decile,
    sku_weight_kg,
    sku_category,
    move_id,
    move_operator,
    move_move_type,
    CONCAT_WS(' / ', sku_category, move_move_type) AS sku_category_move_move_type_label,
    ROUND(sku_weight_kg * 1.1, 2) AS sku_weight_kg_adj
FROM exploded_detail_moves
ORDER BY unit_price_filled
