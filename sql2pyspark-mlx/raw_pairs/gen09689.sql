SELECT s.id, s.store_name
FROM stores s
WHERE s.region IN ('Central', 'North', 'West') AND s.id IN (SELECT store_id FROM store_sales)
