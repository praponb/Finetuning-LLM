SELECT
    ss.id,
    ss.revenue,
    ss.store_id,
    ss.sale_date
FROM store_sales ss
WHERE ss.revenue BETWEEN 2200 AND 2300 AND ss.store_id IN (SELECT id FROM stores WHERE MONTH(opened_date) IN (7, 9, 10))
ORDER BY ss.store_id, id
LIMIT 5;
