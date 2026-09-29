WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        CONCAT(move_type, '#', CAST(qty AS STRING)) AS move_type_ref,
        SUBSTRING(TRIM(operator), 1, 4) AS operator_prefix
    FROM stock_moves
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        unit_price,
        move_type_ref,
        operator_prefix,
        SUM(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        MAX(unit_price) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_unit_price,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_price DESC, id), 3) AS unit_price_percent_rank
    FROM scoped_moves
),
enriched AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.move_type,
        s.moved_at,
        s.operator_prefix,
        s.running_unit_price,
        s.warehouse_id_max_unit_price,
        s.unit_price_percent_rank,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM sequenced s
    JOIN warehouses w ON w.id = s.warehouse_id
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', operator_prefix, move_type) AS operator_prefix_move_type_label
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, unit_price_percent_rank, running_unit_price, warehouse_id_max_unit_price
    FROM annotated
) src
UNPIVOT (
    amount FOR metric IN (unit_price_percent_rank, running_unit_price, warehouse_id_max_unit_price)
) unp
ORDER BY id, metric
