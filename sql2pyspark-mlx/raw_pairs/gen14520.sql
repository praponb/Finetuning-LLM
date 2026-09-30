SELECT store_id, SUM(quantity) AS total_quantity, DENSE_RANK() OVER (ORDER BY SUM(quantity) DESC) AS rnk
FROM inventory
GROUP BY store_id
ORDER BY total_quantity DESC
