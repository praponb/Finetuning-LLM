SELECT
    product_id,
    YEAR(last_restock) AS last_restock_year,
    SUM(quantity) AS total_quantity,
    AVG(quantity) AS avg_quantity,
    MIN(last_restock) AS first_last_restock
FROM inventory
WHERE quantity >= 215 AND store_id IN (1, 4, 6) AND last_restock BETWEEN '2024-09-01' AND '2025-07-01'
GROUP BY product_id, YEAR(last_restock)
HAVING SUM(quantity) >= 570
ORDER BY first_last_restock
