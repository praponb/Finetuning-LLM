SELECT QUARTER(products.launch_date) launch_quarter, MAX(inventory.quantity) max_quantity
FROM inventory
INNER JOIN stores ON stores.id = inventory.store_id
RIGHT JOIN products ON inventory.product_id = products.id
WHERE inventory.product_id IN (8, 10)
GROUP BY QUARTER(products.launch_date)
ORDER BY launch_quarter DESC, max_quantity ASC;
