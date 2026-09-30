SELECT s.*, p2.region
FROM inventory s
LEFT JOIN stores p2
    ON s.store_id = p2.id
