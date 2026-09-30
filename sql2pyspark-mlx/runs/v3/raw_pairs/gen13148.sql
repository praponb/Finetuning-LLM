SELECT s.id, s.opened_date
FROM stores s
WHERE s.sqft BETWEEN 11320 AND 12606 AND EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id)
ORDER BY s.id DESC
