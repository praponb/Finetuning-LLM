SELECT *
FROM (
    SELECT product_id, ROUND(AVG(quantity), 2) avg_quantity, MIN(quantity) min_quantity
    FROM inventory
    WHERE MONTH(last_restock) IN (4, 9, 12)
    GROUP BY product_id
) agg
WHERE min_quantity > 120;
