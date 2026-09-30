WITH filtered_inventory AS (
    SELECT *
    FROM inventory
    WHERE last_restock BETWEEN '2023-05-01' AND '2024-02-16'
),
agg AS (
    SELECT product_id, MIN(quantity) AS min_quantity
    FROM filtered_inventory
    GROUP BY product_id
)
SELECT *
FROM agg
WHERE min_quantity >= 150
ORDER BY 1 DESC;
