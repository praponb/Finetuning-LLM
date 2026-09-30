SELECT i.store_id, ss.sale_date
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue BETWEEN 2300 AND 2400
ORDER BY i.store_id DESC
