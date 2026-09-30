SELECT
    ss.id,
    ss.product_id,
    ss.store_id,
    ss.units
FROM store_sales ss
WHERE ss.store_id IN (SELECT id FROM stores WHERE city IN ('Bangkok', 'Singapore', 'Chiang Mai'));
