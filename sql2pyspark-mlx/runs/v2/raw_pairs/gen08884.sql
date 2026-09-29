SELECT
    ss.id,
    ss.revenue,
    ss.store_id,
    ss.sale_date
FROM store_sales ss
WHERE ss.store_id IN (SELECT id FROM stores WHERE city = 'New York')
ORDER BY revenue DESC;
