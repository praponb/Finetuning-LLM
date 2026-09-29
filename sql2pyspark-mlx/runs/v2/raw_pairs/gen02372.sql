SELECT product_id, AVG(quantity) AS avg_quantity
FROM inventory
GROUP BY 1;
