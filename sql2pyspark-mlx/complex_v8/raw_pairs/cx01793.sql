SELECT move_type, warehouse_id AS ref_id, moved_at AS event_time
FROM stock_moves
UNION ALL
SELECT operator AS move_type, warehouse_id AS ref_id, moved_at AS event_time
FROM stock_moves
