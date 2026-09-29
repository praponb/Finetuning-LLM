SELECT
    store_id,
    COUNT(DISTINCT quantity) AS unique_products,
    COUNT(DISTINCT quantity) AS unique_stores,
    MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
WHERE last_restock BETWEEN '2023-04-01' AND '2024-04-01'
GROUP BY store_id
HAVING MAX(quantity) - MIN(quantity) < 60
ORDER BY 3 DESC;
