SELECT
    s.opened_date,
    s.store_name,
    ss.units,
    ss.product_id,
    ss.revenue
FROM stores s
INNER JOIN store_sales ss ON s.id = ss.store_id
ORDER BY ss.revenue DESC
