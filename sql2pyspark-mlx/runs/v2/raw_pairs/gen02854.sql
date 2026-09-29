SELECT
    ss.id,
    ss.units,
    ss.store_id,
    ss.sale_date
FROM store_sales ss
WHERE ss.store_id IN (SELECT id FROM stores WHERE store_name NOT LIKE 'Ai%');
