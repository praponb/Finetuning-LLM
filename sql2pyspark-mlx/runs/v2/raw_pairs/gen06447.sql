SELECT store_id, SUM(quantity) AS total_quantity, RANK() OVER (ORDER BY SUM(quantity) DESC) AS rnk FROM inventory WHERE quantity > 127 GROUP BY store_id
