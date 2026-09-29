SELECT
    s.id,
    s.city,
    s.store_name,
    s.region
FROM stores s
WHERE LENGTH(s.store_name) < 13 AND EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.last_restock BETWEEN '2024-03-07' AND '2025-10-04')
