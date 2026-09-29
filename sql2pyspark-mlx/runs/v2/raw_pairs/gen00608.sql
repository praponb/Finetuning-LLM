SELECT store_id, avg_quantity, latest_last_restock
FROM (
    SELECT store_id, AVG(quantity) AS avg_quantity, MAX(last_restock) AS latest_last_restock
    FROM inventory
    GROUP BY store_id
) AS sub
WHERE avg_quantity >= 170;
