SELECT products.brand, ROUND(STDDEV(inventory.quantity), 2) AS stddev_quantity, MIN(inventory.last_restock) AS min_last_restock
FROM inventory
INNER JOIN products ON inventory.product_id = products.id
WHERE inventory.last_restock < '2023-10-13'
GROUP BY products.brand;
