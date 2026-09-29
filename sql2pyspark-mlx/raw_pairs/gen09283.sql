SELECT *
FROM (
    SELECT store_id, product_id, COUNT(*) AS num_inventory
    FROM inventory
    GROUP BY store_id, product_id
) AS agg
WHERE num_inventory >= 10
ORDER BY 3 DESC
