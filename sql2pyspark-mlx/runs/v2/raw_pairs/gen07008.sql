SELECT ss.product_id, MAX(i.quantity) max_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units IS NOT NULL
GROUP BY ss.product_id;
