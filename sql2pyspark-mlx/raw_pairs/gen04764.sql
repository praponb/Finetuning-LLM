SELECT product_id, store_id, total_quantity
FROM (
    SELECT product_id, store_id, SUM(quantity) AS total_quantity
    FROM inventory
    GROUP BY product_id, store_id
) AS agg
WHERE total_quantity < 470
ORDER BY store_id DESC;
