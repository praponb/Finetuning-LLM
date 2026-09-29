WITH inventory_by_product AS (
    SELECT product_id, MIN(last_restock) AS first_last_restock, MAX(quantity) AS max_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.price,
    p.cost,
    ibp.first_last_restock,
    ibp.max_quantity
FROM products p
JOIN inventory_by_product ibp ON p.id = ibp.product_id
ORDER BY price;
