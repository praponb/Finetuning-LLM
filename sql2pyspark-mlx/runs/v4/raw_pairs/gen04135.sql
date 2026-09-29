WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        CASE WHEN quantity >= 242 THEN 'High' WHEN quantity >= 98 THEN 'Medium' ELSE 'Low' END quantity_tier
    FROM inventory
    WHERE product_id IN (4, 10)
)
SELECT id, store_id
FROM valid
INTERSECT ALL
SELECT id, store_id
FROM inventory
WHERE YEAR(last_restock) >= 2024
