SELECT
    s.id,
    s.sqft,
    s.store_name,
    s.region
FROM stores s
WHERE s.city = 'New York' AND s.id IN (SELECT store_id FROM store_sales WHERE product_id IN (2, 7))
