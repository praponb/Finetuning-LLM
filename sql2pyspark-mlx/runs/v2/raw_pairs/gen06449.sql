SELECT COUNT(DISTINCT quantity) AS unique_products, MIN(last_restock) AS min_last_restock FROM inventory WHERE last_restock BETWEEN '2023-07-01' AND '2023-08-01' AND store_id IN (3, 4, 5, 6);
