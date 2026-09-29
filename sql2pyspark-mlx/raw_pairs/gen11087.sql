WITH src AS (
    SELECT *, CAST(quantity AS DOUBLE) AS quantity_str
    FROM inventory
    WHERE (product_id IN (1, 4, 6, 9) AND YEAR(last_restock) >= 2024) OR store_id = 3
),
last_restock_quarter_summary AS (
    SELECT QUARTER(last_restock) AS last_restock_quarter, MIN(quantity) AS min_quantity
    FROM src
    GROUP BY QUARTER(last_restock)
)
SELECT *
FROM last_restock_quarter_summary
WHERE min_quantity > 160
