WITH product_totals AS (
    SELECT product_id, MAX(last_restock) AS latest_last_restock, MIN(quantity) AS min_quantity
    FROM inventory
    WHERE quantity BETWEEN 104 AND 117
    GROUP BY product_id
)
SELECT
    p.id,
    p.launch_date,
    p.supplier_id,
    pt.latest_last_restock,
    pt.min_quantity
FROM products p
JOIN product_totals pt ON p.id = pt.product_id;
