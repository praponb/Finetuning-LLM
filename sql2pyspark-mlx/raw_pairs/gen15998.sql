WITH InventoryByProduct AS (
    SELECT product_id, COUNT(DISTINCT quantity) AS unique_stores
    FROM inventory
    GROUP BY product_id
)
SELECT p.id, p.brand, I.unique_stores
FROM products p
INNER JOIN InventoryByProduct I ON p.id = I.product_id
ORDER BY unique_stores DESC;
