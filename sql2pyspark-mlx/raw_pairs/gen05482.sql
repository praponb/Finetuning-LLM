WITH base AS (
    SELECT *, CASE WHEN quantity >= 243 THEN 'High' WHEN quantity >= 47 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE quantity = 194
),
agg AS (
    SELECT store_id, ROUND(AVG(quantity), 2) AS avg_quantity, COUNT(quantity) AS quantity_count
    FROM base
    GROUP BY store_id
)
SELECT store_id, avg_quantity
FROM agg
ORDER BY store_id DESC, avg_quantity DESC
