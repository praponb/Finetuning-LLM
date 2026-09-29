WITH src AS (
    SELECT *
    FROM inventory
    WHERE last_restock BETWEEN '2023-03-01' AND '2025-02-28' AND product_id = 9
),
store_id_summary AS (
    SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM src
    GROUP BY store_id
)
SELECT *
FROM store_id_summary
