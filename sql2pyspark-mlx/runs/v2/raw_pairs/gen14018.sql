SELECT
    DATE_TRUNC('MONTH', last_restock) AS last_restock_month,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity,
    COUNT(*) AS inventory_count
FROM inventory
GROUP BY 1
HAVING COUNT(*) > 4
ORDER BY stddev_quantity DESC;
