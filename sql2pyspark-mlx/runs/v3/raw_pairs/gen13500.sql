WITH recent_inventory AS (
    SELECT *
    FROM inventory
    WHERE (quantity <= 109 AND quantity BETWEEN 72 AND 73) OR store_id IN (1, 3, 6)
),
agg AS (
    SELECT product_id, AVG(quantity) AS avg_quantity
    FROM recent_inventory
    GROUP BY product_id
)
SELECT *
FROM agg
WHERE avg_quantity > 180
ORDER BY product_id DESC, avg_quantity
