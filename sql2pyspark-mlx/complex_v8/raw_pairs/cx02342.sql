WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        UPPER(TRIM(operator)) AS operator_clean,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix,
        CONCAT(move_type, '#', CAST(qty AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE unit_price < 317.0
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        qty,
        moved_at,
        unit_price,
        operator_clean,
        move_type_prefix,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY sku_id, warehouse_id), 0), 4) AS unit_price_share_of_sku_id_warehouse_id,
        LEAD(qty) OVER (ORDER BY qty DESC, id) AS next_qty,
        ROW_NUMBER() OVER (ORDER BY qty DESC, id) AS qty_seq,
        ROUND(AVG(unit_price) OVER (ORDER BY qty DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_7
    FROM prepared_moves
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY qty_seq ORDER BY unit_price), 3) AS unit_price_percent_rank,
        ROUND(unit_price_avg_7 - AVG(unit_price_avg_7) OVER (PARTITION BY qty_seq), 2) AS unit_price_avg_7_vs_qty_seq_avg
    FROM ranked
)
-- Final projection
SELECT
    id,
    qty,
    unit_price,
    operator_clean,
    move_type_prefix,
    unit_price_share_of_sku_id_warehouse_id,
    qty_seq,
    unit_price_avg_7,
    unit_price_percent_rank,
    GREATEST(unit_price_avg_7, qty_seq) AS greatest_unit_price_avg_7_qty_seq,
    ROUND(unit_price_percent_rank * 1.5, 2) AS unit_price_percent_rank_adj
FROM ranked_2
ORDER BY unit_price_percent_rank DESC, unit_price_avg_7 NULLS LAST, id
LIMIT 20
