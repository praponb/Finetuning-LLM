SELECT i.quantity, i.product_id, ss.revenue
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units IS NULL
ORDER BY revenue DESC, i.product_id;
