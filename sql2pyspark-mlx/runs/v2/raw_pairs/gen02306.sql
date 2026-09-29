SELECT *
FROM (
    SELECT product_id, MAX(last_restock) AS last_last_restock, SUM(quantity) AS total_quantity
    FROM inventory
    WHERE product_id IN (2, 7, 9, 10)
    GROUP BY product_id
) AS agg
WHERE total_quantity < 480;
