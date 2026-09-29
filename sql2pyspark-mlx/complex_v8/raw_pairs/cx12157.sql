SELECT warehouse_id, moved_at AS event_time, id AS ref_id
FROM stock_moves
WHERE HOUR(moved_at) BETWEEN 7 AND 11
UNION ALL
SELECT warehouse_id, moved_at, qty
FROM stock_moves
WHERE unit_price >= 206.8
  AND sku_id <= 18
