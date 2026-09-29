WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE move_type IN ('ADJUST', 'INBOUND')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY move_type_or_unknown)) AS distinct_move_type_in_move_type_or_unknown,
        MAX(qty) OVER (PARTITION BY move_type_or_unknown) AS move_type_or_unknown_max_qty,
        MAX(unit_price_bucket) OVER (ORDER BY qty DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_bucket_max_2_prior,
        ROUND(unit_price_bucket / NULLIF(SUM(unit_price_bucket) OVER (PARTITION BY move_type_or_unknown), 0), 4) AS unit_price_bucket_share_of_move_type_or_unknown
    FROM recent
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', move_type_or_unknown, move_type) AS move_type_or_unknown_move_type_label,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day
    FROM with_trends
)
-- Final projection
SELECT
    id,
    sku_id,
    qty,
    move_type_or_unknown,
    distinct_move_type_in_move_type_or_unknown,
    move_type_or_unknown_max_qty,
    unit_price_bucket_max_2_prior,
    unit_price_bucket_share_of_move_type_or_unknown,
    moved_at_day,
    LENGTH(TRIM(move_type_or_unknown)) AS move_type_or_unknown_len
FROM tagged
