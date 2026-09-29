SELECT
    DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month,
    SUM(quantity) AS total_quantity,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    MAX(quantity) AS max_quantity
FROM inventory
WHERE quantity = 102
GROUP BY DATE_FORMAT(last_restock, 'yyyy-MM')
ORDER BY max_quantity, quantity_range DESC;
