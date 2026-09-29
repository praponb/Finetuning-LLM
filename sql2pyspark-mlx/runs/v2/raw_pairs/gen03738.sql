SELECT
    ss.id,
    ss.sale_date,
    ss.store_id,
    ss.revenue
FROM store_sales ss
WHERE ss.store_id IN (SELECT id FROM stores WHERE opened_date >= '2024-05-01')
ORDER BY revenue DESC;
