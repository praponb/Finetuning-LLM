WITH src AS (
    SELECT *
    FROM inventory
    WHERE store_id = 2
),
agg AS (
    SELECT DATE_TRUNC('MONTH', last_restock) AS last_restock_month, ROUND(AVG(quantity), 2) AS avg_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM src
    GROUP BY DATE_TRUNC('MONTH', last_restock)
)
SELECT *
FROM agg
WHERE quantity_range < 23;
