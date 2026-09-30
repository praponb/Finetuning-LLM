SELECT ss.product_id, MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units != 18
GROUP BY ss.product_id
ORDER BY ss.product_id DESC;
