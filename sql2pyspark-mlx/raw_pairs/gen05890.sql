SELECT product_id, total_quantity
FROM (
    SELECT product_id, SUM(quantity) AS total_quantity
    FROM inventory
    WHERE quantity = 229
    GROUP BY product_id
) AS t
WHERE total_quantity >= 630
ORDER BY product_id DESC;
