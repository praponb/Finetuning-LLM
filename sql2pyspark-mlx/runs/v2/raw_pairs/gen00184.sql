WITH base AS (
    SELECT *
    FROM inventory
    WHERE product_id IN (2, 3, 5, 9) AND quantity BETWEEN 89 AND 134
),
inventory_stats AS (
    SELECT store_id, MAX(quantity) - MIN(quantity) quantity_range, MAX(quantity) max_quantity
    FROM base
    GROUP BY store_id
)
SELECT store_id, quantity_range, max_quantity
FROM inventory_stats
