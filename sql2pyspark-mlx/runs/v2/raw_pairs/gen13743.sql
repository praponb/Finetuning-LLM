SELECT
    product_id,
    store_id,
    MONTH(last_restock) AS last_restock_month,
    MAX(last_restock) AS last_last_restock
FROM inventory
WHERE store_id IN (1, 6) AND quantity IS NOT NULL
GROUP BY product_id, store_id, last_restock_month
ORDER BY last_restock_month DESC;
