SELECT YEAR(last_restock) AS last_restock_year, product_id, COUNT(DISTINCT quantity) AS unique_stores
FROM inventory
WHERE store_id = 3 AND store_id IN (1, 4, 5, 6) AND quantity < 141
GROUP BY last_restock_year, product_id
HAVING unique_stores < 3;
