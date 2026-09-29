WITH inventory_by_product AS (
    SELECT product_id, AVG(quantity) AS avg_quantity, MAX(quantity) AS max_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT p.stock_qty, ibp.avg_quantity, ibp.max_quantity
FROM products p
LEFT JOIN inventory_by_product ibp ON p.id = ibp.product_id
WHERE ibp.max_quantity >= 220
ORDER BY avg_quantity
