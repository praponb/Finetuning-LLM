WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        moved_at + INTERVAL 1 DAYS AS moved_at_plus_1d,
        CASE WHEN move_type IN ('TRANSFER', 'ADJUST') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-24 12:00:00'
),
grouped AS (
    -- Group-level totals
    SELECT
        warehouse_id,
        ROUND(SUM(qty) / NULLIF(SUM(unit_price), 0), 3) AS qty_to_unit_price_ratio,
        MAX(unit_price) AS max_unit_price,
        COUNT(*) FILTER (WHERE move_type NOT IN ('TRANSFER', 'ADJUST')) AS n_move_type_not_in_transfer_ad,
        SUM(unit_price) FILTER (WHERE move_type = 'ADJUST') AS unit_price_move_type_adjust,
        SUM(unit_price) AS total_unit_price
    FROM prepared
    GROUP BY warehouse_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(max_unit_price, 2) OVER (ORDER BY total_unit_price, warehouse_id) AS next2_max_unit_price,
        ROW_NUMBER() OVER (ORDER BY total_unit_price, warehouse_id) AS total_unit_price_seq
    FROM grouped
)
SELECT
    warehouse_id,
    max_unit_price,
    n_move_type_not_in_transfer_ad,
    unit_price_move_type_adjust,
    total_unit_price,
    next2_max_unit_price,
    LEAST(qty_to_unit_price_ratio, total_unit_price_seq) AS least_qty_to_unit_price_ratio_total_unit_price_s,
    ROUND(total_unit_price * 1.5, 2) AS total_unit_price_adj
FROM sequenced
ORDER BY total_unit_price_adj DESC, least_qty_to_unit_price_ratio_total_unit_price_s DESC
