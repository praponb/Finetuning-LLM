WITH inventory_by_product AS (
    SELECT product_id, MIN(quantity) AS min_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT p.stock_qty, p.product_name, ibp.min_quantity
FROM products p
LEFT JOIN inventory_by_product ibp ON p.id = ibp.product_id
WHERE ibp.min_quantity > 170;
