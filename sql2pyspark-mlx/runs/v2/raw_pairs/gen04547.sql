WITH InventoryByProduct AS (
    SELECT product_id, ROUND(STDDEV(quantity), 2) stddev_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT p.id, p.brand, I.stddev_quantity
FROM products p
INNER JOIN InventoryByProduct I ON p.id = I.product_id;
