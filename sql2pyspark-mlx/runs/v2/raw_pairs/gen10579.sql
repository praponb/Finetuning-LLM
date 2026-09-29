SELECT
    ss.id,
    ss.units,
    ss.sale_date,
    ss.store_id
FROM store_sales ss
WHERE ss.revenue < 3800 AND ss.store_id IN (SELECT id FROM stores WHERE region = 'Central');
