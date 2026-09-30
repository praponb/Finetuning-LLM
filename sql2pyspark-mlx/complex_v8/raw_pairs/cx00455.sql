WITH all_events AS (
    -- Stack the branches
    SELECT
        'move' AS source,
        sku_id AS key_id,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE qty >= 78
    UNION
    SELECT
        'move_2' AS source,
        sku_id,
        moved_at,
        unit_price
    FROM stock_moves
)
SELECT source, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id, MAX(moved_at) AS latest_moved_at
FROM all_events
GROUP BY source
ORDER BY source
