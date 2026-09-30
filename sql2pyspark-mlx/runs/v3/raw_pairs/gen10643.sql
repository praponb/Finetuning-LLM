SELECT store_id, SUM(quantity) AS total_quantity, DENSE_RANK() OVER (ORDER BY SUM(quantity) DESC) AS quantity_rank FROM inventory GROUP BY store_id ORDER BY quantity_rank;
