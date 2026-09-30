SELECT qty AS ref_id, moved_at AS event_time
FROM stock_moves
UNION ALL
SELECT qty AS ref_id, moved_at AS event_time
FROM stock_moves
ORDER BY ref_id DESC
