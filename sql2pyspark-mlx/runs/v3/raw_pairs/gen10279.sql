SELECT i.id, i.last_restock, ss.units
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue > 3300 OR i.store_id IN (2, 3, 5, 6)
