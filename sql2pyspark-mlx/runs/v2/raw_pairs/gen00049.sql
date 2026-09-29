SELECT
    i.last_restock,
    ss.sale_date,
    ss.revenue,
    ss.store_id
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY last_restock ASC, revenue DESC
