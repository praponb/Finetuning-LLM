SELECT s.id, s.store_name, s.opened_date
FROM stores s
WHERE s.id IN (SELECT store_id FROM inventory)
