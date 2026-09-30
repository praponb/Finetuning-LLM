WITH recent_inventory AS (
    SELECT *
    FROM inventory
    WHERE last_restock BETWEEN '2024-06-01' AND '2024-12-06'
),
Summary AS (
    SELECT product_id, COUNT(quantity) AS quantity_count
    FROM recent_inventory
    GROUP BY product_id
)
SELECT product_id, quantity_count
FROM Summary
WHERE quantity_count < 4
ORDER BY product_id DESC, quantity_count
