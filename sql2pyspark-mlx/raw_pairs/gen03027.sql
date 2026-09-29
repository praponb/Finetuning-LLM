SELECT
    store_id,
    product_id,
    QUARTER(last_restock) AS last_restock_quarter,
    MAX(quantity) AS max_quantity,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    AVG(quantity) AS avg_quantity
FROM inventory
WHERE quantity IS NOT NULL AND store_id = 2
GROUP BY store_id, product_id, last_restock_quarter
