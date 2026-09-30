SELECT *
FROM (
    SELECT DATE_TRUNC('MONTH', last_restock) AS last_restock_month, AVG(quantity) AS avg_quantity
    FROM inventory
    WHERE product_id IN (5, 6, 7, 8)
    GROUP BY DATE_TRUNC('MONTH', last_restock)
) AS sub
WHERE avg_quantity > 170;
