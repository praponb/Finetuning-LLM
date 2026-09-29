SELECT product_id, COUNT(quantity) AS quantity_count FROM inventory WHERE quantity BETWEEN 201 AND 208 GROUP BY 1;
