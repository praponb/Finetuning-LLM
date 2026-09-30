WITH InventoryByProduct AS (
    SELECT product_id, SUM(quantity) AS total_quantity, COUNT(*) AS total_count
    FROM inventory
    WHERE last_restock <= '2023-09-18'
    GROUP BY product_id
)
SELECT
    p.supplier_id,
    p.price,
    I.total_quantity,
    I.total_count
FROM products p
INNER JOIN InventoryByProduct I ON p.id = I.product_id
WHERE I.total_quantity >= 960;
