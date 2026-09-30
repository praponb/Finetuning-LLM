SELECT
    QUARTER(last_restock) AS last_restock_quarter,
    MAX(last_restock) AS last_last_restock,
    MAX(quantity) AS max_quantity,
    MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
GROUP BY QUARTER(last_restock)
