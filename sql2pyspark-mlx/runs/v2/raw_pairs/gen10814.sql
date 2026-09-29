SELECT last_restock FROM inventory WHERE last_restock > '2024-01-01' UNION ALL SELECT last_restock FROM inventory WHERE last_restock BETWEEN '2024-07-15' AND '2025-08-18';
