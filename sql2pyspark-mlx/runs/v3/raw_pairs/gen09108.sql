SELECT ss.product_id, MIN(i.quantity) AS min_quantity, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE MONTH(ss.sale_date) IN (2, 3, 10)
GROUP BY ss.product_id
