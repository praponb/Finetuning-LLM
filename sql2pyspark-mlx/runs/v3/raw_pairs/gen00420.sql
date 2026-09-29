SELECT DATE_FORMAT(stores.opened_date, 'yyyy-MM') AS opened_month, ROUND(STDDEV(inventory.quantity), 2) AS stddev_quantity
FROM inventory
LEFT JOIN stores ON inventory.store_id = stores.id
GROUP BY DATE_FORMAT(stores.opened_date, 'yyyy-MM')
