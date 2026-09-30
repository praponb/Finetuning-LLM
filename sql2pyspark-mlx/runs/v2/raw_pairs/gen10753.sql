SELECT s.id, s.store_name, s.opened_date
FROM stores s
WHERE s.city = 'New York' AND s.id IN (SELECT store_id FROM store_sales)
