WITH recent_inventory AS (
    SELECT *, FLOOR(quantity) AS quantity_whole
    FROM inventory
    WHERE quantity < 169
),
Summary AS (
    SELECT store_id, COUNT(quantity) AS quantity_count
    FROM recent_inventory
    GROUP BY store_id
)
SELECT *
FROM Summary
WHERE quantity_count < 9;
