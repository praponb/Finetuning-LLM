SELECT stores.region, DATE_TRUNC('MONTH', stores.opened_date) AS opened_month, ROUND(STDDEV(inventory.quantity), 2) AS stddev_quantity
FROM inventory
RIGHT JOIN stores ON inventory.store_id = stores.id
WHERE stores.store_name LIKE '%Downtown%'
GROUP BY stores.region, DATE_TRUNC('MONTH', stores.opened_date)
ORDER BY stddev_quantity DESC;
