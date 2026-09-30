SELECT
    DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM inventory
WHERE (MONTH(last_restock) IN (1, 4, 11) AND store_id = 2) OR last_restock <= '2025-07-12'
GROUP BY DATE_FORMAT(last_restock, 'yyyy-MM')
