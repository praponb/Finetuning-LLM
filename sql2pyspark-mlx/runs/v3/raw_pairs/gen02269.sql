SELECT
    p.id,
    p.brand,
    p.supplier_id,
    t.min_quantity,
    t.quantity_range
FROM products p
LEFT JOIN (SELECT product_id, MIN(quantity) AS min_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    WHERE MONTH(last_restock) IN (1, 4, 5)
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.quantity_range >= 59;
