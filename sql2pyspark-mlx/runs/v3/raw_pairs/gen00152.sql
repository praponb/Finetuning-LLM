SELECT
    ss.id,
    ss.product_id,
    s.city,
    s.store_name
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
WHERE s.store_name LIKE '%Downtow%' OR ss.revenue IS NULL;
