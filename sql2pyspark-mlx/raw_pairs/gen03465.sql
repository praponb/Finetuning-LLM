SELECT store_id, total_quantity, quantity_range
FROM (
    SELECT store_id, SUM(quantity) AS total_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    WHERE last_restock BETWEEN '2024-01-01' AND '2025-01-01'
    GROUP BY store_id
) AS s
WHERE quantity_range > 68;
