SELECT ss.product_id, DATE_TRUNC('MONTH', ss.sale_date) AS sale_month, MAX(i.last_restock) AS latest_last_restock
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.store_id IN (1, 2, 3, 6)
GROUP BY ss.product_id, DATE_TRUNC('MONTH', ss.sale_date)
