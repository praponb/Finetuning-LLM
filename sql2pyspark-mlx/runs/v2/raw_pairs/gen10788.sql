SELECT *
FROM (
    SELECT product_id, QUARTER(last_restock) AS last_restock_quarter, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    GROUP BY product_id, QUARTER(last_restock)
) AS s
WHERE quantity_range >= 36;
