SELECT *
FROM (
    SELECT
        product_id,
        store_id,
        AVG(quantity) AS avg_quantity,
        MIN(quantity) AS min_quantity
    FROM inventory
    GROUP BY product_id, store_id
) AS agg
WHERE avg_quantity < 190
ORDER BY product_id DESC;
