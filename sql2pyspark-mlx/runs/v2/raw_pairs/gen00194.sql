SELECT *
FROM (
    SELECT DATE_TRUNC('MONTH', last_restock) AS last_restock_month, MAX(quantity) AS max_quantity, COUNT(DISTINCT quantity) AS unique_products
    FROM inventory
    WHERE quantity < 122
    GROUP BY DATE_TRUNC('MONTH', last_restock)
) AS s
WHERE max_quantity >= 90;
