SELECT
    s.id,
    s.opened_date,
    s.sqft,
    s.region
FROM stores s
WHERE s.id IN (SELECT store_id FROM inventory WHERE MONTH(last_restock) IN (1, 4, 11));
