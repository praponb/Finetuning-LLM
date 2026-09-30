SELECT DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month, MAX(last_restock) AS last_last_restock, MAX(quantity) AS max_quantity
FROM inventory
GROUP BY DATE_FORMAT(last_restock, 'yyyy-MM')
