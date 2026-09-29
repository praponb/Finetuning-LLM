SELECT store_id, product_id, avg_quantity
FROM (
    SELECT store_id, product_id, AVG(quantity) avg_quantity
    FROM inventory
    GROUP BY store_id, product_id
) t
WHERE avg_quantity > 210
ORDER BY 3 DESC;
