SELECT ss.store_id, MIN(i.last_restock) AS min_last_restock
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE MONTH(ss.sale_date) IN (2, 3, 4)
GROUP BY ss.store_id
ORDER BY store_id;
