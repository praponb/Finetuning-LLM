SELECT s.id, s.region
FROM stores s
WHERE s.city IN ('Berlin', 'Chiang Mai') AND NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.quantity < 67)
ORDER BY s.region, id DESC
