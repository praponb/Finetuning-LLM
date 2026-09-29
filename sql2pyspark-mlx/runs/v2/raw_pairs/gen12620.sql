SELECT
    DATE_TRUNC('MONTH', last_restock) AS last_restock_month,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity,
    MAX(last_restock) AS latest_last_restock
FROM inventory
WHERE quantity <= 205 AND product_id IN (3, 4)
GROUP BY last_restock_month
ORDER BY quantity_range;
