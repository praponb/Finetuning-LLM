SELECT YEAR(stores.opened_date) AS opened_year, ROUND(STDDEV(inventory.quantity), 2) AS stddev_quantity
FROM inventory
INNER JOIN products ON inventory.product_id = products.id
LEFT OUTER JOIN stores ON stores.id = inventory.store_id
WHERE inventory.store_id IN (3, 4, 5)
GROUP BY YEAR(stores.opened_date)
HAVING ROUND(STDDEV(inventory.quantity), 2) > 70;
