SELECT * FROM inventory WHERE (store_id IN (4, 6) AND quantity <= 212) OR last_restock BETWEEN '2023-11-06' AND '2025-08-08' ORDER BY last_restock;
