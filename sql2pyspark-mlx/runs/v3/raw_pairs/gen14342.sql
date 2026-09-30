SELECT product_id, SUM(quantity) AS total_quantity, RANK() OVER (ORDER BY SUM(quantity) DESC) AS quantity_rank FROM inventory WHERE quantity > 65 GROUP BY product_id
