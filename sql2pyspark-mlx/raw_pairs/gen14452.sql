SELECT
    last_restock_year,
    store_id,
    avg_quantity,
    min_last_restock
FROM (
    SELECT
        YEAR(last_restock) AS last_restock_year,
        store_id,
        AVG(quantity) AS avg_quantity,
        MIN(last_restock) AS min_last_restock
    FROM inventory
    GROUP BY YEAR(last_restock), store_id
) AS s
WHERE avg_quantity > 150;
