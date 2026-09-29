WITH inventory_by_product AS (
    SELECT product_id, MIN(quantity) AS min_quantity, SUM(quantity) AS total_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT p.product_name, ibp.min_quantity, ibp.total_quantity
FROM products p
INNER JOIN inventory_by_product ibp ON p.id = ibp.product_id
WHERE ibp.total_quantity >= 730
ORDER BY product_name;
