WITH FilteredInventory AS (
    SELECT *
    FROM inventory
    WHERE quantity <> 107
),
last_restock_year_summary AS (
    SELECT YEAR(last_restock) AS last_restock_year, AVG(quantity) AS avg_quantity
    FROM FilteredInventory
    GROUP BY YEAR(last_restock)
)
SELECT *
FROM last_restock_year_summary
WHERE avg_quantity < 80
