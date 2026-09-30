SELECT ROUND(AVG(quantity), 2) AS avg_quantity FROM inventory WHERE last_restock BETWEEN '2023-12-01' AND '2025-10-01';
