WITH inventory_by_product AS (
    SELECT product_id, SUM(quantity) AS total_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    WHERE store_id IN (4, 6)
    GROUP BY product_id
)
SELECT
    p.id,
    p.launch_date,
    p.supplier_id,
    ibp.total_quantity,
    ibp.quantity_range
FROM products p
INNER JOIN inventory_by_product ibp ON p.id = ibp.product_id;
