SELECT store_id, YEAR(last_restock) AS last_restock_year, COUNT(*) AS num_inventory
FROM inventory
WHERE quantity BETWEEN 148 AND 218 AND last_restock < '2024-07-01'
GROUP BY 1, 2;
