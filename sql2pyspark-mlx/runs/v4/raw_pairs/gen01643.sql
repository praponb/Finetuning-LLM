SELECT
    ss.id,
    ss.revenue,
    ss.units,
    ss.sale_date
FROM store_sales ss
WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE YEAR(opened_date) >= 2024);
