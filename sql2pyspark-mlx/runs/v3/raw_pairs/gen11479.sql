SELECT
    QUARTER(last_restock) AS last_restock_quarter,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    MAX(quantity) AS max_quantity,
    COUNT(DISTINCT quantity) AS unique_products
FROM inventory
GROUP BY 1;
