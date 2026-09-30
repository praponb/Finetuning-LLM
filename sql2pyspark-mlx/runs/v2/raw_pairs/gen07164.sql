SELECT
    s.id,
    s.sqft,
    s.store_name,
    s.opened_date
FROM stores s
WHERE s.id IN (SELECT store_id FROM store_sales)
ORDER BY opened_date DESC
