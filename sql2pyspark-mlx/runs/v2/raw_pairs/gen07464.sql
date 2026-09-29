SELECT s.id, s.store_name, s.opened_date
FROM stores s
WHERE s.region IN ('North', 'Central', 'East') AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id)
ORDER BY id, opened_date
LIMIT 10;
