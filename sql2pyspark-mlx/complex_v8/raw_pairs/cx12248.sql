WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE qty > 199
      AND move_type NOT IN ('OUTBOUND', 'INBOUND')
)
SELECT warehouse_id AS entity_id, moved_at AS event_time
FROM filtered
WHERE sku_id >= 12
  AND warehouse_id <= 3
UNION
SELECT qty AS entity_id, moved_at AS event_time
FROM stock_moves
WHERE qty BETWEEN 67 AND 393
  AND moved_at >= TIMESTAMP '2025-01-19 12:00:00'
ORDER BY entity_id, event_time
