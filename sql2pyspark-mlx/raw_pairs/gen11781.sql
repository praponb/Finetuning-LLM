SELECT
    store_id,
    COUNT(quantity) quantity_count,
    MIN(last_restock) min_last_restock,
    ROUND(AVG(quantity), 2) avg_quantity
FROM inventory
WHERE last_restock <= '2024-01-07' OR product_id IN (2, 3, 10)
GROUP BY store_id
HAVING COUNT(DISTINCT quantity) >= 5
ORDER BY store_id DESC;
