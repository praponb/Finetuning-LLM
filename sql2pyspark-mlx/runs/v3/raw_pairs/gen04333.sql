SELECT
    ss.id,
    ss.units,
    ss.product_id,
    ss.store_id
FROM store_sales ss
WHERE ss.product_id IN (5, 9) AND ss.store_id IN (SELECT id FROM stores WHERE sqft BETWEEN 10129 AND 11882);
