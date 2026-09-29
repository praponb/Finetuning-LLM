SELECT
    products.category,
    products.brand,
    MIN(inventory.quantity) AS min_quantity,
    ROUND(STDDEV(inventory.quantity), 2) AS stddev_quantity
FROM inventory
INNER JOIN products ON inventory.product_id = products.id
WHERE YEAR(inventory.last_restock) = 2024
GROUP BY products.category, products.brand
HAVING ROUND(STDDEV(inventory.quantity), 2) >= 77
