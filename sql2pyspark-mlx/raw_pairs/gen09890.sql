SELECT store_id, COUNT(DISTINCT quantity) AS unique_stores
FROM inventory
GROUP BY store_id
ORDER BY unique_stores ASC;
