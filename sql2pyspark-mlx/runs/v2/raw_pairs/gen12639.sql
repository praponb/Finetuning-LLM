WITH inventory_by_product AS (
    SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.supplier_id,
    p.stock_qty,
    ibp.stddev_quantity,
    ibp.quantity_range
FROM products p
INNER JOIN inventory_by_product ibp ON p.id = ibp.product_id;
