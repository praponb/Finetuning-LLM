SELECT operator AS name, moved_at AS event_time, move_type AS name_2
FROM stock_moves
UNION
SELECT operator, moved_at, move_type
FROM stock_moves
