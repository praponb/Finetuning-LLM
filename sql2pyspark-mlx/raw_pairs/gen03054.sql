WITH inventory_by_product AS (
    SELECT product_id, AVG(quantity) AS avg_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT p.supplier_id, ibp.avg_quantity
FROM products p
LEFT OUTER JOIN inventory_by_product ibp ON p.id = ibp.product_id
