SELECT i.id, i.last_restock
FROM inventory i
WHERE i.product_id NOT IN (SELECT id FROM products WHERE YEAR(launch_date) = 2025)
ORDER BY 2 DESC, id DESC
