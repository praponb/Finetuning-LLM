WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE unit_price < 313.8
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type), 2) AS unit_price_vs_move_type_avg,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type
    FROM recent
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        unit_price,
        unit_price_vs_move_type_avg,
        rows_in_move_type,
        COUNT(*) OVER (PARTITION BY move_type, sku_id) AS rows_in_move_type_sku_id,
        LAG(unit_price_vs_move_type_avg) OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price ASC NULLS LAST, id) AS prev_unit_price_vs_move_type_avg,
        ROW_NUMBER() OVER w_warehouse_id_sku_id_by_unit AS unit_price_seq,
        NTILE(5) OVER w_warehouse_id_sku_id_by_unit AS unit_price_quintile
    FROM ranked
    WINDOW w_warehouse_id_sku_id_by_unit AS (PARTITION BY warehouse_id, sku_id ORDER BY unit_price ASC NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    sku_id,
    unit_price,
    prev_unit_price_vs_move_type_avg,
    unit_price_seq,
    ROUND(unit_price / NULLIF(rows_in_move_type, 0), 3) AS unit_price_per_rows_in_move_type,
    unit_price_seq + rows_in_move_type_sku_id AS unit_price_seq_plus_rows_in_move_type_sku_id
FROM with_trends
ORDER BY unit_price DESC
