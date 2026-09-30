WITH inventory_by_product AS (
    SELECT product_id, COUNT(quantity) AS quantity_count
    FROM inventory
    GROUP BY product_id
)
SELECT p.price, ibp.quantity_count
FROM products p
INNER JOIN inventory_by_product ibp ON p.id = ibp.product_id
WHERE ibp.quantity_count >= 6
