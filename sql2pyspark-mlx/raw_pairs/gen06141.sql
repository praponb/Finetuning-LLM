SELECT product_id, last_last_restock, avg_quantity
FROM (
    SELECT product_id, MAX(last_restock) AS last_last_restock, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM inventory
    GROUP BY product_id
) AS t
WHERE avg_quantity < 51
ORDER BY 2
