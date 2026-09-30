SELECT DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM inventory
GROUP BY last_restock_month
ORDER BY stddev_quantity DESC, last_restock_month
LIMIT 3;
