SELECT product_id, avg_quantity
FROM (
    SELECT product_id, ROUND(AVG(quantity), 2) avg_quantity
    FROM inventory
    GROUP BY product_id
) t
WHERE avg_quantity > 74
ORDER BY product_id DESC;
