WITH FilteredInventory AS (
    SELECT *, ROUND(quantity, 1) AS quantity_rounded
    FROM inventory
    WHERE product_id IN (1, 2)
),
summary AS (
    SELECT product_id, store_id, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM FilteredInventory
    GROUP BY product_id, store_id
)
SELECT *
FROM summary;
