SELECT
    ss.id,
    ss.revenue,
    ss.units,
    ss.store_id
FROM store_sales ss
WHERE ss.revenue IS NULL AND ss.store_id NOT IN (SELECT id FROM stores WHERE city = 'Singapore')
ORDER BY ss.id;
