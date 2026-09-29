WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        last_restock,
        DATEDIFF(CURRENT_DATE(), last_restock) / 7 AS weeks_since_last_restock
    FROM inventory
    WHERE store_id = 2
)
SELECT store_id
FROM valid
EXCEPT DISTINCT
SELECT store_id
FROM inventory
WHERE last_restock BETWEEN '2023-06-01' AND '2025-04-18'
