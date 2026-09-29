SELECT i.quantity, ss.store_id
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id IN (3, 5, 6, 7) AND ss.units IS NOT NULL
ORDER BY quantity DESC;
