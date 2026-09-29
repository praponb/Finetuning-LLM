SELECT s.id, s.opened_date, s.store_name
FROM stores s
WHERE s.id IN (SELECT store_id FROM store_sales);
