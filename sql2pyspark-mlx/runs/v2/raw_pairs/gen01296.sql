SELECT product_id, unique_products, avg_quantity
FROM (
    SELECT product_id, COUNT(DISTINCT quantity) AS unique_products, AVG(quantity) AS avg_quantity
    FROM inventory
    GROUP BY product_id
) AS t
WHERE avg_quantity < 200
